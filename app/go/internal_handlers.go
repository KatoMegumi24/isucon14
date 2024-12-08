package main

import (
	"database/sql"
	"errors"
	"net/http"
)

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// 1. 未割り当てのライドを取得 (古い順に1件)
	// FOR UPDATE を付与してこのライドに対する同時マッチング処理を防止
	ride := &Ride{}
	err = tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 未割り当てライドなし
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 2. 利用可能な椅子候補を取得
	// 椅子候補条件:
	// - is_active = TRUE
	// - 現在未完了のライドにアサインされていないこと
	//
	// 「未完了ライドにアサインされていない」判定方法:
	// ride_statusesから、chair_idに紐づくライドの最新状態がCOMPLETED以外のものがなければOK。
	//
	// ここでは chairsテーブルとchair_models、chair_locations(最新位置)を結合して取得します。
	//
	// 最新位置は chair_locations から chair_id 毎に最も新しいレコードをJOINするためにサブクエリを使います。
	// 未完了ライドの存在判定はNOT EXISTSでチェックします。
	query := `
SELECT c.id, c.owner_id, c.name, c.model, c.is_active, c.access_token, c.created_at, c.updated_at,
       c.total_distance, c.total_distance_updated_at, c.last_longitude, c.last_latitude,
       cm.speed,
       cl.latitude AS current_latitude, cl.longitude AS current_longitude
FROM chairs c
JOIN chair_models cm ON c.model = cm.name
JOIN (
    SELECT cc.chair_id, cc.latitude, cc.longitude FROM chair_locations cc
    INNER JOIN (
        SELECT chair_id, MAX(created_at) AS max_created
        FROM chair_locations
        GROUP BY chair_id
    ) latest ON cc.chair_id = latest.chair_id AND cc.created_at = latest.max_created
) cl ON cl.chair_id = c.id
WHERE c.is_active = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM rides r
    JOIN (
      SELECT rs.ride_id, rs.status
      FROM ride_statuses rs
      INNER JOIN (
        SELECT ride_id, MAX(created_at) AS max_created
        FROM ride_statuses
        GROUP BY ride_id
      ) t ON rs.ride_id = t.ride_id AND rs.created_at = t.max_created
    ) lr ON r.id = lr.ride_id
    WHERE r.chair_id = c.id
      AND lr.status != 'COMPLETED'
  )
`
	type CandidateChair struct {
		Chair
		Speed            int `db:"speed"`
		CurrentLatitude  int `db:"current_latitude"`
		CurrentLongitude int `db:"current_longitude"`
	}

	candidates := []CandidateChair{}
	if err := tx.SelectContext(ctx, &candidates, query); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(candidates) == 0 {
		// 利用可能な椅子がない
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 3. 各候補椅子について到着予測時間を計算し、最も早く到着できる椅子を選択
	// 到着予測時間 ≒ マンハッタン距離 / スピード（スピードが大きいほど早い）
	// スピードが0のケースは無いと仮定（モデル定義より）
	bestChair := candidates[0]
	bestTime := arrivalTime(bestChair.Speed, bestChair.CurrentLatitude, bestChair.CurrentLongitude,
		ride.PickupLatitude, ride.PickupLongitude)

	for _, chair := range candidates[1:] {
		t := arrivalTime(chair.Speed, chair.CurrentLatitude, chair.CurrentLongitude,
			ride.PickupLatitude, ride.PickupLongitude)
		if t < bestTime {
			bestTime = t
			bestChair = chair
		}
	}

	// 4. ライドに椅子を割り当て
	_, err = tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", bestChair.ID, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// arrivalTime は単純な到着推定時間を返す
func arrivalTime(speed, cLat, cLon, rLat, rLon int) float64 {
	dist := float64(abs(cLat-rLat) + abs(cLon-rLon))
	// speedあたり1距離進むと考えると、time = distance/speed
	return dist / float64(speed)
}

package main

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/oklog/ulid/v2"
)

// internalGetMatching: 500msに一度呼ばれる想定。待っている全てのライドに対して椅子を割り当てる。
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// MATCHING状態でまだchair_idがNULLのライドを全て取得
	// ライドの最新ステータスがMATCHINGでchair_idがNULLのものを取得する
	// (最新ステータス取得にはサブクエリを使用)
	rides := []Ride{}
	err = tx.SelectContext(ctx, &rides, `
		SELECT r.* FROM rides r
		INNER JOIN (
			SELECT ride_id, MAX(created_at) AS max_created FROM ride_statuses GROUP BY ride_id
		) rs_max ON rs_max.ride_id = r.id
		INNER JOIN ride_statuses rs ON rs.ride_id = r.id AND rs.created_at = rs_max.max_created
		WHERE rs.status = 'MATCHING' AND r.chair_id IS NULL
		ORDER BY r.created_at
	`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 待ちライドなし
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// すでに他のライド(未COMPLETED)に割り当てられていない、かつis_active = trueな椅子を取得
	// 最新ステータスがCOMPLETED以外のライドを持つ椅子は除外
	// 以下のサブクエリで "chairs" の中から自由なもののみ抽出
	chairsWithModel := []struct {
		ID       string        `db:"id"`
		Model    string        `db:"model"`
		IsActive bool          `db:"is_active"`
		LastLat  sql.NullInt64 `db:"last_latitude"`
		LastLon  sql.NullInt64 `db:"last_longitude"`
		Speed    int           `db:"speed"`
	}{}

	err = tx.SelectContext(ctx, &chairsWithModel, `
		SELECT c.id, c.model, c.is_active, c.last_latitude, c.last_longitude, cm.speed
		FROM chairs c
		INNER JOIN chair_models cm ON c.model = cm.name
		WHERE c.is_active = TRUE
		AND c.id NOT IN (
			SELECT DISTINCT r2.chair_id 
			FROM rides r2
			INNER JOIN (
				SELECT ride_id, MAX(created_at) AS max_created FROM ride_statuses GROUP BY ride_id
			) t ON t.ride_id = r2.id
			INNER JOIN ride_statuses rs2 ON rs2.ride_id = r2.id AND rs2.created_at = t.max_created
			WHERE rs2.status != 'COMPLETED' AND r2.chair_id IS NOT NULL
		)
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// chair_id→chair情報マップ
	freeChairs := make(map[string]*struct {
		ID      string
		Speed   int
		LastLat int
		LastLon int
	})
	for _, c := range chairsWithModel {
		if !c.LastLat.Valid || !c.LastLon.Valid {
			// 位置情報がない椅子は割り当て困難なのでスキップする（要件次第で扱いを決める）
			continue
		}
		freeChairs[c.ID] = &struct {
			ID      string
			Speed   int
			LastLat int
			LastLon int
		}{
			ID:      c.ID,
			Speed:   c.Speed,
			LastLat: int(c.LastLat.Int64),
			LastLon: int(c.LastLon.Int64),
		}
	}

	if len(freeChairs) == 0 {
		// 空いている椅子がなければ何もできない
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// ライドごとに最適な椅子を探す
	// 到着予測時間 = (マンハッタン距離) / speed
	// speedが大きいほど早く到着する
	// 最小到着時間の椅子を選択
	assignments := []struct {
		RideID  string
		ChairID string
	}{}

	for _, ride := range rides {
		bestChairID := ""
		bestTime := 1 << 30 // 大きな値で初期化
		for cid, ch := range freeChairs {
			dist := calculateDistance(ch.LastLat, ch.LastLon, ride.PickupLatitude, ride.PickupLongitude)
			if ch.Speed <= 0 {
				// speedが0など異常値の場合スキップ
				continue
			}
			timeEstimate := dist / ch.Speed
			// 同着時は最初のものを採用するが、必要なら速度優先などのルールを追加可
			if timeEstimate < bestTime {
				bestTime = timeEstimate
				bestChairID = cid
			}
		}

		if bestChairID == "" {
			// 適した椅子が無い場合は割り当てなし
			continue
		}

		// 割り当て決定
		assignments = append(assignments, struct {
			RideID  string
			ChairID string
		}{RideID: ride.ID, ChairID: bestChairID})

		// 割り当てた椅子はもう使えないのでfreeChairsから削除
		delete(freeChairs, bestChairID)
		if len(freeChairs) == 0 {
			// もう椅子が無いのでこれ以上割り当て不可能
			break
		}
	}

	if len(assignments) == 0 {
		// 割り当てできなかった
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 割り当て結果をDBに反映
	// 一括でUPDATEとstatus挿入
	// ステータスをENROUTEに変更
	// Rides.chair_idを更新
	var rideIDs []string
	params := []interface{}{}
	for _, asg := range assignments {
		rideIDs = append(rideIDs, asg.RideID)
		// Rides の更新
		params = append(params, asg.ChairID, asg.RideID)
	}
	// 一括UPDATE: バルクUPDATEは複雑なので個別UPDATEする
	// パフォーマンスチューニングするならバルクINSERT、UPDATEを検討
	for _, asg := range assignments {
		// rideにchair_idを割り当て
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", asg.ChairID, asg.RideID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		// ステータスをENROUTEに追加
		newStatusID := ulidMake() // ulidMake()は既存のulid生成関数と同等と仮定
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)",
			newStatusID, asg.RideID, "ENROUTE",
		); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// 内部でULID生成用の関数（既存app_handlers.goなどと同様のもの）
func ulidMake() string {
	return ulid.Make().String()
}

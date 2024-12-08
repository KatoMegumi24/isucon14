package main

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/oklog/ulid/v2"
)

// internalGetMatching: 500msに一度呼ばれる想定。待っている全てのライドに対して椅子を割り当てる。
// 改善点:
// - 複数ライドと複数椅子に対して、個別に最適な組み合わせではなく、
//   全体最適を求めるためにハンガリアン法でマッチングを行う。
// - 速度や距離を考慮した到着予測時間(コスト)に基づき、
//   合計の到着時間が最小となるような割り当てを一括決定する。
// - 利用可能な椅子とライドが同数でない場合は、マトリックスを拡張して不整合を解決し、
//   一部割り当てをしないライドが発生しても良いように対応する。

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// MATCHING状態でchair_idがNULLのライドを全て取得
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
		if errors.Is(err, sql.ErrNoRows) || len(rides) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 空いている椅子を取得（他のライドに割り当てられていない＆is_active = true）
	var chairsWithModel []struct {
		ID       string        `db:"id"`
		Model    string        `db:"model"`
		IsActive bool          `db:"is_active"`
		LastLat  sql.NullInt64 `db:"last_latitude"`
		LastLon  sql.NullInt64 `db:"last_longitude"`
		Speed    int           `db:"speed"`
	}
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

	freeChairs := []struct {
		ID      string
		Speed   int
		LastLat int
		LastLon int
	}{}
	for _, c := range chairsWithModel {
		if !c.LastLat.Valid || !c.LastLon.Valid {
			// 位置情報がない椅子は割り当て困難なのでスキップ
			continue
		}
		if c.Speed <= 0 {
			// 異常なspeedならスキップ
			continue
		}
		freeChairs = append(freeChairs, struct {
			ID      string
			Speed   int
			LastLat int
			LastLon int
		}{
			ID:      c.ID,
			Speed:   c.Speed,
			LastLat: int(c.LastLat.Int64),
			LastLon: int(c.LastLon.Int64),
		})
	}

	if len(freeChairs) == 0 {
		// 空いている椅子がなければ何もできない
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// コスト行列作成: 行=Rides, 列=Chairs
	// コスト=到着予測時間(dist/speed)。speed>0、dist≥0なので問題なし。
	// 行列は正方行列にするため、rides, freeChairsの大きい方に合わせてpaddingを行う。
	n := len(rides)
	m := len(freeChairs)
	size := n
	if m > size {
		size = m
	}

	// largeCost: 非割り当て用の大きなコスト (マッチしない場合を表現)
	const largeCost = 9999999

	costMatrix := make([][]int, size)
	for i := 0; i < size; i++ {
		costMatrix[i] = make([]int, size)
		for j := 0; j < size; j++ {
			if i < n && j < m {
				// dist計算
				dist := calculateDistance(freeChairs[j].LastLat, freeChairs[j].LastLon, rides[i].PickupLatitude, rides[i].PickupLongitude)
				timeEstimate := dist / freeChairs[j].Speed
				costMatrix[i][j] = timeEstimate
			} else {
				// padding部分はマッチ不可として大コスト
				costMatrix[i][j] = largeCost
			}
		}
	}

	// ハンガリアン法で最小コスト割り当てを求める
	assignment := hungarianMethod(costMatrix)

	// assignment[i] = j で、i行(ride)に対する割り当て椅子列jを表す
	// jがm未満でかつcostがlargeCostでなければ有効なマッチングとする
	assignments := []struct {
		RideID  string
		ChairID string
	}{}

	for i, j := range assignment {
		if i < n && j >= 0 && j < m && costMatrix[i][j] < largeCost {
			// マッチング成立
			assignments = append(assignments, struct {
				RideID  string
				ChairID string
			}{RideID: rides[i].ID, ChairID: freeChairs[j].ID})
		}
	}

	if len(assignments) == 0 {
		// 割り当てできなかった
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 割り当て結果をDBに反映
	for _, asg := range assignments {
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", asg.ChairID, asg.RideID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		newStatusID := ulidMake()
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

// ulidMakeは他ファイルと同等のULID生成関数とする
func ulidMake() string {
	return ulid.Make().String()
}

// ハンガリアン法による最小割り当て計算 (簡易実装例)
// costMatrixは正方行列
// 戻り値: assignment[i]=jは行iが列jに割り当てられたことを示す。
// 割り当てがない場合は-1。
// 以下は最小限のハンガリアン法参考実装例であり、本番環境での最適化は別途考慮が必要。
func hungarianMethod(costMatrix [][]int) []int {
	n := len(costMatrix)
	u := make([]int, n+1)
	v := make([]int, n+1)
	p := make([]int, n+1)
	way := make([]int, n+1)

	for i := 1; i <= n; i++ {
		p[0] = i
		j0 := 0
		minv := make([]int, n+1)
		used := make([]bool, n+1)
		for j := 1; j <= n; j++ {
			minv[j] = 1000000000
		}
		for {
			used[j0] = true
			i0 := p[j0]
			j1 := 0
			delta := 1000000000
			for j := 1; j <= n; j++ {
				if !used[j] {
					cur := costMatrix[i0-1][j-1] - u[i0] - v[j]
					if cur < minv[j] {
						minv[j] = cur
						way[j] = j0
					}
					if minv[j] < delta {
						delta = minv[j]
						j1 = j
					}
				}
			}
			for j := 0; j <= n; j++ {
				if used[j] {
					u[p[j]] += delta
					v[j] -= delta
				} else {
					minv[j] -= delta
				}
			}
			j0 = j1
			if p[j0] == 0 {
				break
			}
		}
		for {
			j1 := way[j0]
			p[j0] = p[j1]
			j0 = j1
			if j0 == 0 {
				break
			}
		}
	}

	res := make([]int, n)
	for j := 1; j <= n; j++ {
		res[p[j]-1] = j - 1
	}
	return res
}

// 以下は既存処理

func ulidMakeString() string {
	return ulid.Make().String()
}

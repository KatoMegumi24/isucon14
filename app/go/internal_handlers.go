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

	// 空いている椅子を取得
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
			continue
		}
		if c.Speed <= 0 {
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
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// costMatrix作成
	// cost = (dist(chair→pickup) + dist(pickup→destination)) / speed
	// これにより、長距離移動が必要なユーザーには、速い椅子が有利になる
	n := len(rides)
	m := len(freeChairs)
	size := n
	if m > size {
		size = m
	}

	const largeCost = 9999999
	costMatrix := make([][]int, size)
	for i := 0; i < size; i++ {
		costMatrix[i] = make([]int, size)
		for j := 0; j < size; j++ {
			if i < n && j < m {
				chair := freeChairs[j]
				ride := rides[i]
				distToPickup := calculateDistance(chair.LastLat, chair.LastLon, ride.PickupLatitude, ride.PickupLongitude)
				distToDestination := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
				totalDist := distToPickup + distToDestination * 2
				costMatrix[i][j] = totalDist / chair.Speed
			} else {
				costMatrix[i][j] = largeCost
			}
		}
	}

	assignment := hungarianMethod(costMatrix)

	assignments := []struct {
		RideID  string
		ChairID string
	}{}

	for i, j := range assignment {
		if i < n && j >= 0 && j < m && costMatrix[i][j] < largeCost {
			assignments = append(assignments, struct {
				RideID  string
				ChairID string
			}{RideID: rides[i].ID, ChairID: freeChairs[j].ID})
		}
	}

	if len(assignments) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	for _, asg := range assignments {
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", asg.ChairID, asg.RideID); err != nil {
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

// ハンガリアン法の実装例（前回答参照）
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

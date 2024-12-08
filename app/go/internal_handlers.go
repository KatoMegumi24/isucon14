package main

import (
	"database/sql"
	"errors"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	query := `
	WITH available_chairs AS (
			SELECT chairs.id AS chair_id
			FROM chairs
			WHERE is_active = TRUE
			AND NOT EXISTS (
					SELECT 1
					FROM ride_statuses rs
					JOIN rides r ON rs.ride_id = r.id
					WHERE r.chair_id = chairs.id
					GROUP BY rs.ride_id
					HAVING COUNT(rs.chair_sent_at) = 6
			)
			ORDER BY RAND()
			LIMIT 1
	)
	SELECT rides.id AS ride_id, available_chairs.chair_id
	FROM rides
	CROSS JOIN available_chairs
	WHERE rides.chair_id IS NULL
	ORDER BY rides.created_at
	LIMIT 1
`
	type Match struct {
		RideID  int `db:"ride_id"`
		ChairID int `db:"chair_id"`
	}

	match := &Match{}
	if err := db.GetContext(ctx, match, query); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// マッチングされた chair_id を rides テーブルに更新
	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", match.ChairID, match.RideID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	/* 	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	   	ride := &Ride{}
	   	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
	   		if errors.Is(err, sql.ErrNoRows) {
	   			w.WriteHeader(http.StatusNoContent)
	   			return
	   		}
	   		writeError(w, http.StatusInternalServerError, err)
	   		return
	   	}

	   	matched := &Chair{}
	   	empty := false
	   	for i := 0; i < 10; i++ {
	   		if err := db.GetContext(ctx, matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
	   			if errors.Is(err, sql.ErrNoRows) {
	   				w.WriteHeader(http.StatusNoContent)
	   				return
	   			}
	   			writeError(w, http.StatusInternalServerError, err)
	   		}

	   		if err := db.GetContext(ctx, &empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
	   			writeError(w, http.StatusInternalServerError, err)
	   			return
	   		}
	   		if empty {
	   			break
	   		}
	   	}
	   	if !empty {
	   		w.WriteHeader(http.StatusNoContent)
	   		return
	   	}

	   	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
	   		writeError(w, http.StatusInternalServerError, err)
	   		return
	   	} */

	w.WriteHeader(http.StatusNoContent)
}

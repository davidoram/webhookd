package core

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/google/uuid"
)

type HandlerContext struct {
	Db *sql.DB
}

func (hctx HandlerContext) PostSubscriptionHandler(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		slog.Error("Error reading request body", slog.Any("error", err))
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	now := time.Now().In(time.UTC)
	sub, err := NewSubscriptionFromJSON(uuid.New(), body, now, now)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		// Check if the error is of type CustomError
		if vErr, ok := err.(govalidator.Errors); ok {
			slog.Error("validation error", slog.Any("error", vErr.Errors()))
			for _, e := range vErr.Errors() {
				fmt.Fprintf(w, "%s\n", e)
			}
		} else {
			slog.Error("error", slog.Any("error", err))
			fmt.Fprintf(w, "Invalid request body")
		}
		return
	}

	err = InsertSubscription(ctx, hctx.Db, sub)
	if err != nil {
		slog.Error("Error inserting subscription", slog.Any("error", err))
		http.Error(w, "Error inserting subscription", http.StatusInternalServerError)
		return
	}

	body, err = sub.MarshallForAPI()
	if err != nil {
		slog.Error("Error marshalling subscription", slog.Any("error", err))
		http.Error(w, "Error marshalling subscription", http.StatusInternalServerError)
		return
	}
	slog.Info("Subscription added", slog.Any("subscription", sub.ID))

	// Set the Location header to the URL of the newly created resource
	w.Header().Set("Location", sub.ResourcePath())

	w.WriteHeader(http.StatusCreated)
	w.Write(body)
}

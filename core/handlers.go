package core

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

func PostSubscriptionHandler(w http.ResponseWriter, r *http.Request) {
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

	var s Subscription
	err = json.Unmarshal(body, &s)
	if err != nil {
		slog.Error("Error parsing JSON request body", slog.Any("error", err))
		http.Error(w, "Error parsing JSON request body", http.StatusBadRequest)
		return
	}

	s.ID = uuid.New()
	slog.Info("Subscription added", slog.Any("subscription", s))

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Subscription added ok")
}

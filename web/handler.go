package web

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/davidoram/webhookd/adapter"
	"github.com/davidoram/webhookd/core"
	"github.com/davidoram/webhookd/view"
	"github.com/google/uuid"
)

type HandlerContext struct {
	Db *sql.DB
}

// PostSubscriptionHandler handles POST requests to create a new subscription
// It takes a JSON body representing the subscription to create
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
	vsub, err := view.NewSubscriptionFromJSON(uuid.New(), body, now, now, sql.NullTime{})
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

	// Transform the view.Subscription into a core.Subscription
	csub, err := adapter.ViewToCoreAdapter(vsub)
	if err != nil {
		slog.Error("Error transforming subscription", slog.Any("error", err))
		http.Error(w, "Error transforming subscription", http.StatusInternalServerError)
		return
	}

	err = core.InsertSubscription(ctx, hctx.Db, csub)
	if err != nil {
		slog.Error("Error inserting subscription", slog.Any("error", err))
		http.Error(w, "Error inserting subscription", http.StatusInternalServerError)
		return
	}

	vSub, err := adapter.CoreToViewAdapter(csub)
	if err != nil {
		slog.Error("Error converting to view", slog.Any("error", err))
		http.Error(w, "Error converting to view", http.StatusInternalServerError)
		return
	}

	body, err = json.Marshal(vSub)
	if err != nil {
		slog.Error("Error marshalling subscription", slog.Any("error", err))
		http.Error(w, "Error marshalling subscription", http.StatusInternalServerError)
		return
	}
	slog.Info("Subscription added", slog.Any("id", vsub.ID), slog.String("name", vsub.Name))

	// Set the Location header to the URL of the newly created resource
	w.Header().Set("Location", csub.ResourcePath())

	w.WriteHeader(http.StatusCreated)
	w.Write(body)
}

// ListSubscriptionsHandler handles GET requests to list subscriptions
// It takes the following query parameters:
// - offset: the offset to start listing subscriptions from, defaults to 0
// - limit: the maximum number of subscriptions to return, defaults to 100
// Example: GET /subscriptions?offset=10&limit=10
func (hctx HandlerContext) ListSubscriptionsHandler(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// retrieve the offset & limit values from URL param
	offset, ok := ParseQueryValue(r, w, "offset", 0)
	if !ok {
		return
	}
	limit, ok := ParseQueryValue(r, w, "limit", 100)
	if !ok {
		return
	}

	subs, err := core.GetSubscriptions(ctx, hctx.Db, offset, limit)
	if err != nil {
		slog.Error("Error reading subscriptions", slog.Any("error", err))
		http.Error(w, "Error reading subscriptions", http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(subs)
	if err != nil {
		slog.Error("Error marshalling subscription", slog.Any("error", err))
		http.Error(w, "Error marshalling subscription", http.StatusInternalServerError)
		return
	}
	slog.Info("List Subscriptions", slog.Any("offset", offset), slog.Any("limit", limit), slog.Any("subscriptions found", len(subs.Subscriptions)))

	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func ParseQueryValue(r *http.Request, w http.ResponseWriter, key string, value int64) (int64, bool) {
	valueStr := r.URL.Query().Get(key)
	if valueStr != "" {
		_, err := fmt.Sscanf(valueStr, "%d", &value)
		if err != nil {
			slog.Error("Error parsing URL param as int64", slog.Any("error", err), slog.String("key", key), slog.String("value", valueStr))
			http.Error(w, fmt.Sprintf("Error parsing URL param '%s' with value '%s' as integer", key, valueStr), http.StatusBadRequest)
			return 0, false
		}
	}
	return value, true
}

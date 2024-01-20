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
	"github.com/jba/muxpatterns"
	"gopkg.in/guregu/null.v4"
)

type HandlerContext struct {
	Db *sql.DB
}

// PostSubscriptionHandler handles POST requests to create a new subscription
// It takes a JSON body representing the subscription to create.
// If the subscription is created successfully, it returns a 201 Created response with the subscription in the body.
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

	// Transform the view.Subscription into a core.Subscription, just to check that it is valid
	csub, err := adapter.ViewToCoreAdapter(vsub)
	if err != nil {
		slog.Error("Error transforming subscription", slog.Any("error", err))
		http.Error(w, "Error transforming subscription", http.StatusInternalServerError)
		return
	}

	err = core.InsertSubscription(ctx, hctx.Db, vsub)
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
	slog.Info("subscription new", slog.Any("id", vsub.ID), slog.String("name", vsub.Name), slog.String("location", csub.ResourcePath()))

	// Set the Location header to the URL of the newly created resource
	w.Header().Set("Location", csub.ResourcePath())

	w.WriteHeader(http.StatusCreated)
	w.Write(body)
}

// ShowSubscriptionHandler handles GET requests to get a single subscription identified by id
// If the subscription is found, it returns a 200 OK response with the subscription in the body.
func (hctx HandlerContext) ShowSubscriptionHandler(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	idStr := muxpatterns.PathValue(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		slog.Error("error parsing URL param as UUID", slog.Any("error", err), slog.String("id", idStr))
		http.Error(w, "Error parsing URL param as UUID", http.StatusBadRequest)
		return
	}

	vsub, found, err := core.GetSubscriptionById(ctx, hctx.Db, id)
	if err != nil {
		slog.Error("error reading subscription", slog.Any("error", err))
		http.Error(w, "Error reading subscription", http.StatusInternalServerError)
		return
	}
	// If we didn't find the subscription, return a 404
	if !found {
		slog.Info("subscription not found", slog.Any("id", id))
		http.Error(w, "Subscription not found", http.StatusNotFound)
		return
	}

	body, err := json.Marshal(vsub)
	if err != nil {
		slog.Error("error marshalling subscription", slog.Any("error", err))
		http.Error(w, "Error marshalling subscription", http.StatusInternalServerError)
		return
	}
	slog.Info("subscription show", slog.Any("id", vsub.ID), slog.Any("name", vsub.Name))
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// DeleteSubscriptionHandler handles DELETE requests to remove a single subscription identified by id
// If the subscription is found, it will (soft) delete it and return a 200 OK response with the subscription in the body.
func (hctx HandlerContext) DeleteSubscriptionHandler(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	idStr := muxpatterns.PathValue(r, "id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		slog.Error("error parsing URL param as UUID", slog.Any("error", err), slog.String("id", idStr))
		http.Error(w, "Error parsing URL param as UUID", http.StatusBadRequest)
		return
	}

	vsub, found, err := core.GetSubscriptionById(ctx, hctx.Db, id)
	if err != nil {
		slog.Error("error reading subscription", slog.Any("error", err))
		http.Error(w, "Error reading subscription", http.StatusInternalServerError)
		return
	}
	// If we didn't find the subscription, return a 404
	if !found {
		slog.Info("subscription not found", slog.Any("id", id))
		http.Error(w, "Subscription not found", http.StatusNotFound)
		return
	}

	// Is found, so mark as deleted in the database
	now := time.Now().In(time.UTC)
	vsub.DeletedAt = null.Time{sql.NullTime{Time: now, Valid: true}}
	vsub.UpdatedAt = now
	err = core.UpdateSubscription(ctx, hctx.Db, vsub)
	if err != nil {
		slog.Error("error updating subscription", slog.Any("error", err), slog.Any("id", id))
		http.Error(w, "Error updating subscription", http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(vsub)
	if err != nil {
		slog.Error("error marshalling subscription", slog.Any("error", err))
		http.Error(w, "Error marshalling subscription", http.StatusInternalServerError)
		return
	}
	slog.Info("subscription delete", slog.Any("id", vsub.ID), slog.Any("name", vsub.Name))
	w.WriteHeader(http.StatusOK)
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
	slog.Info("subscription list", slog.Any("offset", offset), slog.Any("limit", limit), slog.Any("count", len(subs.Subscriptions)))

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

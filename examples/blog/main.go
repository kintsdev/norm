package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kintsdev/norm"
)

// User represents a blog user
type User struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	Name      string    `db:"name" norm:"not_null,varchar(100)"`
	Email     string    `db:"email" norm:"unique,not_null,varchar(255)"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
	UpdatedAt time.Time `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
}

// Post represents a blog post
type Post struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	UserID    int64     `db:"user_id" norm:"not_null,fk:users(id),on_delete:cascade"`
	Title     string    `db:"title" norm:"not_null,varchar(200)"`
	Content   string    `db:"content" norm:"not_null"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
	UpdatedAt time.Time `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
}

// Comment represents a comment on a post
type Comment struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	PostID    int64     `db:"post_id" norm:"not_null,fk:posts(id),on_delete:cascade"`
	UserID    int64     `db:"user_id" norm:"not_null,fk:users(id),on_delete:cascade"`
	Body      string    `db:"body" norm:"not_null"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
	UpdatedAt time.Time `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&User{}, &Post{}, &Comment{}); err != nil {
		log.Fatalf("automigrate: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) { usersHandler(kn, w, r) })
	mux.HandleFunc("/users/", func(w http.ResponseWriter, r *http.Request) { userByIDHandler(kn, w, r) })
	mux.HandleFunc("/posts", func(w http.ResponseWriter, r *http.Request) { postsHandler(kn, w, r) })
	mux.HandleFunc("/posts/", func(w http.ResponseWriter, r *http.Request) { postByIDHandler(kn, w, r) })
	mux.HandleFunc("/comments", func(w http.ResponseWriter, r *http.Request) { commentsHandler(kn, w, r) })
	mux.HandleFunc("/comments/", func(w http.ResponseWriter, r *http.Request) { commentByIDHandler(kn, w, r) })

	log.Println("blog example listening on :8080")
	if err := http.ListenAndServe(":8080", withJSONHeaders(mux)); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func withJSONHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// USERS
func usersHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo := norm.NewRepository[User](kn)
	switch r.Method {
	case http.MethodGet:
		users, err := repo.Find(ctx)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, users)
	case http.MethodPost:
		var in struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		u := &User{Name: in.Name, Email: in.Email}
		if err := repo.Create(ctx, u); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		// fetch the created row to return with ID
		created, err := repo.FindOne(ctx, norm.Eq("email", in.Email))
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

func userByIDHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := parseIDFromPath(r.URL.Path)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	repo := norm.NewRepository[User](kn)
	switch r.Method {
	case http.MethodGet:
		u, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, u)
	case http.MethodPut:
		var in map[string]any
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		// only allow certain fields
		upd := map[string]any{}
		if v, ok := in["name"]; ok {
			if s, ok := v.(string); ok {
				upd["name"] = s
			}
		}
		if v, ok := in["email"]; ok {
			if s, ok := v.(string); ok {
				upd["email"] = s
			}
		}
		if err := repo.UpdatePartial(ctx, id, upd); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		u, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, u)
	case http.MethodDelete:
		if err := repo.Delete(ctx, id); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

// POSTS
func postsHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo := norm.NewRepository[Post](kn)
	switch r.Method {
	case http.MethodGet:
		// optional filter by user_id
		if uidStr := r.URL.Query().Get("user_id"); uidStr != "" {
			uid, err := strconv.ParseInt(uidStr, 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			posts, err := repo.Find(ctx, norm.Eq("user_id", uid))
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, posts)
			return
		}
		posts, err := repo.Find(ctx)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, posts)
	case http.MethodPost:
		var in struct {
			UserID  int64  `json:"user_id"`
			Title   string `json:"title"`
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		p := &Post{UserID: in.UserID, Title: in.Title, Content: in.Content}
		if err := repo.Create(ctx, p); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		// return last created by querying latest by created_at for this user/title
		created, err := repo.FindOne(ctx, norm.Eq("user_id", in.UserID), norm.Eq("title", in.Title))
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

func postByIDHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := parseIDFromPath(r.URL.Path)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	repo := norm.NewRepository[Post](kn)
	switch r.Method {
	case http.MethodGet:
		p, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, p)
	case http.MethodPut:
		var in map[string]any
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		upd := map[string]any{}
		if v, ok := in["title"].(string); ok {
			upd["title"] = v
		}
		if v, ok := in["content"].(string); ok {
			upd["content"] = v
		}
		if v, ok := in["user_id"]; ok {
			switch vv := v.(type) {
			case float64:
				upd["user_id"] = int64(vv)
			case int64:
				upd["user_id"] = vv
			}
		}
		if err := repo.UpdatePartial(ctx, id, upd); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		p, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, p)
	case http.MethodDelete:
		if err := repo.Delete(ctx, id); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

// COMMENTS
func commentsHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	repo := norm.NewRepository[Comment](kn)
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		if pidStr := q.Get("post_id"); pidStr != "" {
			pid, err := strconv.ParseInt(pidStr, 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			cs, err := repo.Find(ctx, norm.Eq("post_id", pid))
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, cs)
			return
		}
		cs, err := repo.Find(ctx)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, cs)
	case http.MethodPost:
		var in struct {
			PostID int64  `json:"post_id"`
			UserID int64  `json:"user_id"`
			Body   string `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		c := &Comment{PostID: in.PostID, UserID: in.UserID, Body: in.Body}
		if err := repo.Create(ctx, c); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		created, err := repo.FindOne(ctx, norm.Eq("post_id", in.PostID), norm.Eq("user_id", in.UserID), norm.Eq("body", in.Body))
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

func commentByIDHandler(kn *norm.KintsNorm, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := parseIDFromPath(r.URL.Path)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	repo := norm.NewRepository[Comment](kn)
	switch r.Method {
	case http.MethodGet:
		c, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, c)
	case http.MethodPut:
		var in map[string]any
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		upd := map[string]any{}
		if v, ok := in["body"].(string); ok {
			upd["body"] = v
		}
		if v, ok := in["post_id"]; ok {
			switch vv := v.(type) {
			case float64:
				upd["post_id"] = int64(vv)
			case int64:
				upd["post_id"] = vv
			}
		}
		if v, ok := in["user_id"]; ok {
			switch vv := v.(type) {
			case float64:
				upd["user_id"] = int64(vv)
			case int64:
				upd["user_id"] = vv
			}
		}
		if err := repo.UpdatePartial(ctx, id, upd); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		c, err := repo.GetByID(ctx, id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, c)
	case http.MethodDelete:
		if err := repo.Delete(ctx, id); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeErr(w, http.StatusMethodNotAllowed, nil)
	}
}

func parseIDFromPath(path string) (int64, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(parts[len(parts)-1], 10, 64)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	if err == nil {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

package admin

import (
	"io/fs"
	"net/http"
	"strings"
)

// spaFileServer returns an http.Handler that serves static files from the
// given fs.FS, falling back to index.html for client-side routes.
func spaFileServer(fsys fs.FS) http.Handler {
	fileServer := http.FileServerFS(fsys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never serve SPA for API paths — return 404 instead.
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Try to open the file. If it exists, serve it.
		f, err := fsys.Open(path)
		if err == nil {
			_ = f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// For anything else (client-side routes), serve index.html.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

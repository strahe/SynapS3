package admin

import "net/http"

func (s *Server) handleAPIMonitoring(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.monitoring == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "monitoring unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, s.monitoring.Snapshot())
}

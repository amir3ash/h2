# HTTP/2 Server in Go — RFC 9113

A small, experimental HTTP/2 server in Go implementing RFC 9113. Designed for low memory per connection using Linux epoll and a reduced-goroutine, event-loop style per-connection architecture. Intended for experimentation and constrained environments, not a drop-in production server.

---

## Notes

- Uses epoll — Linux only (optional).
- Experimental: prioritize memory efficiency over API simplicity. APIs and behavior are unstable and may change soon (including config fields, package paths, and event-loop semantics). Use for testing and research, not production.

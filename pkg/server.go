package pkg

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
)

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) RunForever(port int) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()
	SetRouter(r)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		fmt.Printf("run on port %d \n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Listen for the interrupt signal.
	<-ctx.Done()
	// Restore default behavior on the interrupt signal and notify user of shutdown.
	stop()
	log.Println("stop server, would release all sessions")
	CloseSessions()

}

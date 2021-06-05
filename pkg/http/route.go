package http

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tass-io/scheduler/pkg/http/controller"
)

// RegisterRoute registers http routes
func RegisterRoute(r *gin.Engine) {
	r.Use(cors.New(cors.Config{
		AllowMethods:     []string{"PUT", "POST", "GET", "DELETE"},
		AllowHeaders:     []string{"Origin", "Content-Type"},
		AllowCredentials: true,
		AllowOriginFunc: func(origin string) bool {
			return true
		},
		MaxAge: 12 * time.Hour,
	}))
	v1 := r.Group("/v1")
	{
		workflowRoute := v1.Group("/workflow")
		{
			workflowRoute.POST("/", controller.Invoke)
		}
	}
	r.GET("/metrics", prometheusHandler())
}

func prometheusHandler() gin.HandlerFunc {
	h := promhttp.Handler()

	return func(c *gin.Context) {
		h.ServeHTTP(c.Writer, c.Request)
	}
}

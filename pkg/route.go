package pkg

import "github.com/gin-gonic/gin"

func SetRouter(r *gin.Engine) {
	root := r.Group("/")
	root.POST("openCypher", RunQuery)
	// root.POST("openCypher", RunQueryTest)
}

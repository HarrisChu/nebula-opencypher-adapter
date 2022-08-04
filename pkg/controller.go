package pkg

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type (
	qInuput struct {
		Stmt string `json:"query" binding:"required"`
	}
)

func RunQuery(ctx *gin.Context) {
	var in *qInuput
	ctx.Request.ParseForm()
	for k, v := range ctx.Request.Form {
		if k == "query" {
			in = &qInuput{Stmt: v[0]}
			break
		}
	}

	if in == nil {
		Logger.Error(fmt.Sprintf("cannot parse query"))
		ctx.JSON(http.StatusUnprocessableEntity, fmt.Sprintf("cannot parse query"))
		return
	}
	c := NewClient()
	ds, err := c.Query(in.Stmt)
	if err != nil {
		Logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, err)
		return
	}
	if !ds.IsSucceed() {
		Logger.Error(ds.GetErrorMsg())
		ctx.JSON(http.StatusInternalServerError, ds.GetErrorMsg())
		return
	}
	r, err := ConvertResult(ds)
	if err != nil {
		Logger.Error(err)
		ctx.JSON(http.StatusInternalServerError, ds.GetErrorMsg())
	}

	ctx.JSON(http.StatusOK, r)

	return
}

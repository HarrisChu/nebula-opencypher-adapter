package pkg

import (
	"fmt"

	nebula_go "github.com/vesoft-inc/nebula-go/v3"
)

type (
	Result struct {
		Results []map[string]interface{} `json:"results,omitempty"`
	}

	Node struct {
		ID         string            `json:"~id,omitempty"`
		EntityType string            `json:"~entityType,omitempty"`
		Labels     []string          `json:"~labels,omitempty"`
		Properties map[string]string `json:"~properties,omitempty"`
	}

	Edge struct {
		ID         string            `json:"~id,omitempty"`
		EntityType string            `json:"~entityType,omitempty"`
		Start      string            `json:"~start,omitempty"`
		End        string            `json:"~end,omitempty"`
		Type       string            `json:"~type,omitempty"`
		Properties map[string]string `json:"~properties,omitempty"`
	}
	Path []interface{}
)

func ConvertNode(node *nebula_go.Node) (*Node, error) {
	n := &Node{}
	n.ID = node.GetID().String()
	n.EntityType = "node"
	n.Labels = append(n.Labels, node.GetTags()...)
	n.Properties = make(map[string]string)
	for _, t := range node.GetTags() {
		ps, err := node.Properties(t)
		if err != nil {
			return nil, err
		}
		for name, value := range ps {
			n.Properties[name] = value.String()
		}
		//just display the first tag
		break
	}
	return n, nil
}

func ConvertEdge(edge *nebula_go.Relationship) (*Edge, error) {
	e := &Edge{}
	e.ID = fmt.Sprintf("%s -> %s", edge.GetSrcVertexID().String(), edge.GetDstVertexID().String())
	e.EntityType = "relationship"
	e.Start = edge.GetSrcVertexID().String()
	e.End = edge.GetDstVertexID().String()
	e.Type = edge.GetEdgeName()
	e.Properties = make(map[string]string)
	for n, p := range edge.Properties() {
		e.Properties[n] = p.String()
	}
	return e, nil
}

func ConvertPath(path *nebula_go.PathWrapper) (Path, error) {
	p := make(Path, 0)
	n := len(path.GetNodes()) + len(path.GetRelationships())
	left, right := 0, 0
	nodes, relationships := path.GetNodes(), path.GetRelationships()
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			v := nodes[left]
			n, err := ConvertNode(v)
			if err != nil {
				return nil, err
			}
			p = append(p, n)
			left += 1
		} else {
			r := relationships[right]
			e, err := ConvertEdge(r)
			if err != nil {
				return nil, err
			}
			p = append(p, e)
			right += 1
		}
	}
	return p, nil
}

func ConvertResult(ds *nebula_go.ResultSet) (*Result, error) {
	r := &Result{}
	columns := ds.GetColNames()
	rows := ds.GetRowSize()
	for i := 0; i < rows; i++ {
		record, err := ds.GetRowValuesByIndex(i)
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, col := range columns {
			vp, err := record.GetValueByIndex(i)
			if err != nil {
				return nil, err
			}
			value, err := ConvertValue(vp)
			if err != nil {
				return nil, err
			}
			m[col] = value
			r.Results = append(r.Results, m)
		}
	}
	return r, nil
}

func ConvertValue(v *nebula_go.ValueWrapper) (interface{}, error) {
	var value interface{}

	if v.IsVertex() {
		n, err := v.AsNode()
		if err != nil {
			return nil, err
		}
		value, err = ConvertNode(n)
		if err != nil {
			return nil, err
		}
	} else if v.IsEdge() {
		e, err := v.AsRelationship()
		if err != nil {
			return nil, err
		}
		value, err = ConvertEdge(e)
		if err != nil {
			return nil, err
		}
	} else if v.IsPath() {
		p, err := v.AsPath()
		if err != nil {
			return nil, err
		}
		value, err = ConvertPath(p)
		if err != nil {
			return nil, err
		}
	} else if v.IsList() {
		tmp := make([]interface{}, 0)
		vList, err := v.AsList()
		if err != nil {
			return nil, err
		}
		for _, subV := range vList {
			s, err := ConvertValue(&subV)
			if err != nil {
				return nil, err
			}
			tmp = append(tmp, s)
		}
		value = tmp
	} else if v.IsMap() {
		tmp := make(map[string]interface{})
		vMap, err := v.AsMap()
		if err != nil {
			return nil, err
		}
		for k, v := range vMap {
			subV, err := ConvertValue(&v)
			if err != nil {
				return nil, err
			}
			tmp[k] = subV
		}
		value = tmp

	} else {
		value = v.String()
	}
	return value, nil
}

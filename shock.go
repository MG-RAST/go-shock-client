package shock

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/MG-RAST/golib/httpclient"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// TODO use Token

var WAIT_SLEEP = 30 * time.Second
var WAIT_TIMEOUT = time.Duration(-1) * time.Hour
var SHOCK_TIMEOUT = 60 * time.Second
var DATA_SUFFIX = "?download"

type ShockClient struct {
	Host  string
	Token string
	Debug bool
}

type ShockResponse struct {
	Code int        `bson:"status" json:"status"`
	Data *ShockNode `bson:"data" json:"data"`
	Errs []string   `bson:"error" json:"error"`
}

type ShockResponseGeneric struct {
	Code int         `bson:"status" json:"status"`
	Data interface{} `bson:"data" json:"data"`
	Errs []string    `bson:"error" json:"error"`
}

type ShockQueryResponse struct {
	Code       int         `bson:"status" json:"status"`
	Data       []ShockNode `bson:"data" json:"data"`
	Errs       []string    `bson:"error" json:"error"`
	Limit      int         `bson:"limit" json:"limit"`
	Offset     int         `bson:"offset" json:"offset"`
	TotalCount int         `bson:"total_count" json:"total_count"`
}

type ShockQueryResponseGeneric struct {
	Code       int         `bson:"status" json:"status"`
	Data       interface{} `bson:"data" json:"data"`
	Errs       []string    `bson:"error" json:"error"`
	Limit      int         `bson:"limit" json:"limit"`
	Offset     int         `bson:"offset" json:"offset"`
	TotalCount int         `bson:"total_count" json:"total_count"`
}

type ShockNode struct {
	Id           string             `bson:"id" json:"id"`
	Version      string             `bson:"version" json:"version"`
	File         shockFile          `bson:"file" json:"file"`
	Attributes   interface{}        `bson:"attributes" json:"attributes"`
	Indexes      map[string]IdxInfo `bson:"indexes" json:"indexes"`
	VersionParts map[string]string  `bson:"version_parts" json:"-"`
	Tags         []string           `bson:"tags" json:"tags"`
	Linkages     []linkage          `bson:"linkage" json:"linkages"`
	Priority     int                `bson:"priority" json:"priority"`
	CreatedOn    time.Time          `bson:"created_on" json:"created_on"`
	LastModified time.Time          `bson:"last_modified" json:"last_modified"`
	Expiration   time.Time          `bson:"expiration" json:"expiration"` // 0 means no expiration
	Type         string             `bson:"type" json:"type"`
	Parts        *partsList         `bson:"parts" json:"parts"`
}

type shockFile struct {
	Name         string            `bson:"name" json:"name"`
	Size         int64             `bson:"size" json:"size"`
	Checksum     map[string]string `bson:"checksum" json:"checksum"`
	Format       string            `bson:"format" json:"format"`
	Virtual      bool              `bson:"virtual" json:"virtual"`
	VirtualParts []string          `bson:"virtual_parts" json:"virtual_parts"`
	CreatedOn    time.Time         `bson:"created_on" json:"created_on"`
	Locked       *lockInfo         `bson:"-" json:"locked"`
}

type IdxInfo struct {
	TotalUnits  int64     `bson:"total_units" json:"total_units"`
	AvgUnitSize int64     `bson:"average_unit_size" json:"average_unit_size"`
	CreatedOn   time.Time `bson:"created_on" json:"created_on"`
	Locked      *lockInfo `bson:"-" json:"locked"`
}

type linkage struct {
	Type      string   `bson: "relation" json:"relation"`
	Ids       []string `bson:"ids" json:"ids"`
	Operation string   `bson:"operation" json:"operation"`
}

type lockInfo struct {
	CreatedOn time.Time `bson:"-" json:"created_on"`
	Error     string    `bson:"-" json:"error"`
}

type partsFile []string

type partsList struct {
	Count       int         `bson:"count" json:"count"`
	Length      int         `bson:"length" json:"length"`
	VarLen      bool        `bson:"varlen" json:"varlen"`
	Parts       []partsFile `bson:"parts" json:"parts"`
	Compression string      `bson:"compression" json:"compression"`
}

type Opts map[string]string

func (o *Opts) HasKey(key string) bool {
	if _, has := (*o)[key]; has {
		return true
	}
	return false
}

func (o *Opts) Value(key string) string {
	val, _ := (*o)[key]
	return val
}

func NewShockClient(shock_url string, shock_auth string, debug bool) (sc *ShockClient) {
	sc = &ShockClient{Host: shock_url, Token: shock_auth, Debug: debug}
	return
}

// *** low-level functions ***

func (sc *ShockClient) getRequest(resource string, query url.Values, response interface{}) (err error) {
	return sc.doRequest("GET", resource, query, response)
}

func (sc *ShockClient) putRequest(resource string, query url.Values, response interface{}) (err error) {
	return sc.doRequest("PUT", resource, query, response)
}

func (sc *ShockClient) postRequest(resource string, query url.Values, response interface{}) (err error) {
	return sc.doRequest("POST", resource, query, response)
}

func (sc *ShockClient) deleteRequest(resource string, query url.Values, response interface{}) (err error) {
	return sc.doRequest("DELETE", resource, query, response)
}

// for GET / PUT / POST / DELETE with no multipart from
func (sc *ShockClient) doRequestString(method string, resource string, query url.Values) (jsonstream []byte, err error) {
	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
		return
	}

	(*myurl).Path = resource
	(*myurl).RawQuery = query.Encode()
	shockurl := myurl.String()

	if sc.Debug {
		fmt.Fprintf(os.Stdout, "doRequest url: %s\n", shockurl)
	}
	if len(shockurl) < 5 {
		err = errors.New("could not parse shockurl: " + shockurl)
		return
	}

	var user *httpclient.Auth
	if sc.Token != "" {
		user = httpclient.GetUserByTokenAuth(sc.Token)
	}

	var res *http.Response
	res, err = httpclient.Do(method, shockurl, httpclient.Header{}, nil, user)
	if err != nil {
		return
	}

	defer res.Body.Close()

	jsonstream, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	if sc.Debug {
		fmt.Fprintf(os.Stdout, "json response:\n %s\n", string(jsonstream))
	}

	return
}

// for GET / PUT / POST / DELETE with no multipart from
func (sc *ShockClient) doRequest(method string, resource string, query url.Values, response interface{}) (err error) {
	var jsonstream []byte
	jsonstream, err = sc.doRequestString(method, resource, query)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonstream, response)
	return
}

// for PUT / POST with multipart form
func (sc *ShockClient) CreateOrUpdate(opts Opts, nodeid string, nodeattr map[string]interface{}) (node *ShockNode, err error) {
	host := sc.Host
	token := sc.Token

	if host == "" {
		err = errors.New("(CreateOrUpdate) host is not defined in Shock node")
		return
	}

	url := host + "/node"
	method := "POST"
	if nodeid != "" {
		url += "/" + nodeid
		method = "PUT"
	}
	form := httpclient.NewForm()

	// attributes
	if opts.HasKey("attributes") {
		form.AddFile("attributes", opts.Value("attributes"))
	}
	if len(nodeattr) != 0 {
		var nodeattr_json []byte
		nodeattr_json, err = json.Marshal(nodeattr)
		if err != nil {
			err = errors.New("(CreateOrUpdate) error marshalling NodeAttr")
			return
		}
		form.AddParam("attributes_str", string(nodeattr_json[:]))
	}

	// expiration
	if opts.HasKey("expiration") {
		form.AddParam("expiration", opts.Value("expiration"))
	}
	if opts.HasKey("remove_expiration") {
		form.AddParam("remove_expiration", "1")
	}

	var uploadType string
	if opts.HasKey("upload_type") {
		uploadType = opts.Value("upload_type")
	}

	if uploadType != "" {
		switch uploadType {
		case "basic":
			if opts.HasKey("file") { // upload_type: basic , file=...
				form.AddFile("upload", opts.Value("file"))
			}
			if opts.HasKey("file_name") {
				form.AddParam("file_name", opts.Value("file_name"))
			}
		case "parts":
			if opts.HasKey("parts") {
				form.AddParam("parts", opts.Value("parts"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:parts) missing partial upload parameter: parts")
				return
			}
			if opts.HasKey("file_name") {
				form.AddParam("file_name", opts.Value("file_name"))
			}
		case "part":
			if opts.HasKey("part") && opts.HasKey("file") {
				form.AddFile(opts.Value("part"), opts.Value("file"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:part) missing partial upload parameter: part or file")
				return
			}
		case "remote_path":
			if opts.HasKey("remote_path") {
				form.AddParam("path", opts.Value("remote_path"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:remote_path) missing remote path parameter: path")
				return
			}
		case "virtual_file":
			if opts.HasKey("virtual_file") {
				form.AddParam("type", "virtual")
				form.AddParam("source", opts.Value("virtual_file"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:virtual_file) missing virtual node parameter: source")
				return
			}
		case "copy":
			if opts.HasKey("parent_node") {
				form.AddParam("copy_data", opts.Value("parent_node"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:copy) missing copy node parameter: parent_node")
				return
			}
			if opts.HasKey("copy_indexes") {
				form.AddParam("copy_indexes", "1")
			}
		case "subset":
			if opts.HasKey("parent_node") && opts.HasKey("parent_index") && opts.HasKey("file") {
				form.AddParam("parent_node", opts.Value("parent_node"))
				form.AddParam("parent_index", opts.Value("parent_index"))
				form.AddFile("subset_indices", opts.Value("file"))
			} else {
				err = errors.New("(CreateOrUpdate) (case:subset) missing subset node parameter: parent_node or parent_index or file")
				return
			}
		}
	}

	err = form.Create()
	if err != nil {
		err = fmt.Errorf("(CreateOrUpdate) form.Create returned: %s", err.Error())
		return
	}

	headers := httpclient.Header{
		"Content-Type":   []string{form.ContentType},
		"Content-Length": []string{strconv.FormatInt(form.Length, 10)},
	}
	var user *httpclient.Auth
	if token != "" {
		user = httpclient.GetUserByTokenAuth(token)
	}
	if sc.Debug {
		fmt.Printf("(CreateOrUpdate) url: %s %s\n", method, url)
	}
	var res *http.Response
	res, err = httpclient.Do(method, url, headers, form.Reader, user)
	if err != nil {
		err = fmt.Errorf("(CreateOrUpdate) httpclient.Do returned: %s", err.Error())
		return
	}

	defer res.Body.Close()
	jsonstream, err := ioutil.ReadAll(res.Body)
	if err != nil {
		err = fmt.Errorf("(CreateOrUpdate) ioutil.ReadAll returned: %s", err.Error())
		return
	}
	response := new(ShockResponse)
	err = json.Unmarshal(jsonstream, response)
	if err != nil {
		err = fmt.Errorf("(CreateOrUpdate) (httpclient.Do) failed to marshal response:\"%s\" (err: %s)", jsonstream, err.Error())
		return
	}
	if len(response.Errs) > 0 {
		err = fmt.Errorf("(CreateOrUpdate) type=%s, method=%s, url=%s, error=%s", uploadType, method, url, strings.Join(response.Errs, ","))
		return
	}
	node = response.Data
	return
}

// *** high-level functions ***

func (sc *ShockClient) WaitIndex(nodeid string, indexname string) (index IdxInfo, err error) {
	if indexname == "" {
		return
	}
	var node *ShockNode
	var has bool
	startTime := time.Now()
	for {
		node, err = sc.GetNode(nodeid)
		if err != nil {
			return
		}
		index, has = node.Indexes[indexname]
		if !has {
			err = fmt.Errorf("(shock.WaitIndex) index does not exist: node=%s, index=%s", nodeid, indexname)
			return
		}
		if (index.Locked != nil) && (index.Locked.Error != "") {
			// we have an error, return it
			err = fmt.Errorf("(shock.WaitIndex) error creating index: node=%s, index=%s, error=%s", nodeid, indexname, index.Locked.Error)
			return
		}
		if index.Locked == nil {
			// no longer locked
			return
		}
		// need a resonable timeout
		currTime := time.Now()
		expireTime := currTime.Add(WAIT_TIMEOUT) // adding negative time to current time, because no subtraction function
		if startTime.Before(expireTime) {
			err = fmt.Errorf("(shock.WaitIndex) timeout waiting on index lock: node=%s, index=%s", nodeid, indexname)
			return
		}
		time.Sleep(WAIT_SLEEP)
	}
	return
}

func (sc *ShockClient) WaitFile(nodeid string) (node *ShockNode, err error) {
	startTime := time.Now()
	for {
		node, err = sc.GetNode(nodeid)
		if err != nil {
			return
		}
		if (node.File.Locked != nil) && (node.File.Locked.Error != "") {
			// we have an error, return it
			err = fmt.Errorf("(shock.WaitFile) error waiting on file lock: node=%s, error=%s", nodeid, node.File.Locked.Error)
			return
		}
		if node.File.Locked == nil {
			// no longer locked
			return
		}
		// need a resonable timeout
		currTime := time.Now()
		expireTime := currTime.Add(WAIT_TIMEOUT) // adding negative time to current time, because no subtraction function
		if startTime.Before(expireTime) {
			err = fmt.Errorf("(shock.WaitFile) timeout waiting on file lock: node=%s", nodeid)
			return
		}
		time.Sleep(WAIT_SLEEP)
	}
	return
}

// upload file to node, creates new node if no nodeid given, handle different node types
func (sc *ShockClient) PutOrPostFile(filename string, nodeid string, rank int, attrfile string, ntype string, formopts map[string]string, nodeattr map[string]interface{}) (newNodeid string, err error) {
	opts := Opts{}
	fi, _ := os.Stat(filename)

	if (attrfile != "") && (rank < 2) {
		opts["attributes"] = attrfile
	}
	if filename != "" {
		opts["file"] = filename
		opts["file_name"] = path.Base(filename)
	}
	if rank == 0 {
		opts["upload_type"] = "basic"
	} else {
		opts["upload_type"] = "part"
		opts["part"] = strconv.Itoa(rank)
	}
	if (ntype == "subset") && (rank == 0) && (fi.Size() == 0) {
		opts["upload_type"] = "basic"
	} else if ((ntype == "copy") || (ntype == "subset")) && (len(formopts) > 0) {
		opts["upload_type"] = ntype
		for k, v := range formopts {
			opts[k] = v
		}
	}

	var node *ShockNode
	node, err = sc.CreateOrUpdate(opts, nodeid, nodeattr)
	if err != nil {
		err = fmt.Errorf("(PutFileToShock) failed (%s): %v", sc.Host, err)
		return
	}
	if node != nil {
		newNodeid = node.Id
	}
	return
}

// create basic node with file POST
func (sc *ShockClient) PostFile(filepath string, filename string) (nodeid string, err error) {
	opts := Opts{"upload_type": "basic", "file": filepath}
	if filename != "" {
		opts["file_name"] = filename
	}

	var node *ShockNode
	node, err = sc.createOrUpdate(opts, "", nil)
	if node != nil {
		nodeid = node.Id
	}
	return
}

// create empty node, basic or parts
func (sc *ShockClient) CreateNode(filename string, numParts int) (nodeid string, err error) {
	sr := new(ShockResponse)
	err = sc.postRequest("/node", nil, &sr)
	if err != nil {
		err = fmt.Errorf("(CreateNode) %s", err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(CreateNode) %s", strings.Join(sr.Errs, ","))
		return
	}
	if sr.Data != nil {
		nodeid = sr.Data.Id
	}

	// create "parts" for output splits
	if numParts > 1 {
		opts := Opts{"upload_type": "parts", "file_name": path.Base(filename), "parts": strconv.Itoa(numParts)}
		_, err = sc.createOrUpdate(opts, nodeid, nil)
		if err != nil {
			err = fmt.Errorf("(CreateNode) node=%s: %s", nodeid, err.Error())
		}
	}
	return
}

// update node attributes
func (sc *ShockClient) UpdateAttributes(nodeid string, attrfile string, nodeattr map[string]interface{}) (err error) {
	opts := Opts{}
	if attrfile != "" {
		opts["attributes"] = attrfile
	}
	_, err = sc.createOrUpdate(opts, nodeid, nodeattr)
	return
}

// change node filename
func (sc *ShockClient) UpdateFilename(nodeid string, filename string) (err error) {
	opts := Opts{"upload_type": "basic", "file_name": filename}
	_, err = sc.createOrUpdate(opts, nodeid, nil)
	return
}

// add / modify / delete expiration
func (sc *ShockClient) Expiration(nodeid string, value int, format byte) (err error) {
	opts := Opts{}
	if value == 0 {
		opts["remove_expiration"] = ""
	} else if value > 0 {
		opts["expiration"] = strconv.Itoa(value) + string(format)
	} else {
		return
	}
	_, err = sc.createOrUpdate(opts, nodeid, nil)
	return
}

func (sc *ShockClient) PutIndex(nodeid string, indexname string) (err error) {
	if indexname == "" {
		return
	}
	sr := new(ShockResponseGeneric)
	err = sc.putRequest("/node/"+nodeid+"/index/"+indexname, nil, &sr)
	if err != nil {
		err = fmt.Errorf("(PutIndex) node=%s, index=%s: %s", nodeid, indexname, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(PutIndex) node=%s, index=%s: %s", nodeid, indexname, strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) PutAcl(nodeid string, acltype string, username string) (err error) {
	if (acltype == "") || (username == "") {
		return
	}
	var query url.Values
	query.Add("users", username)

	sr := new(ShockResponseGeneric)
	err = sc.putRequest("/node/"+nodeid+"/acl/"+acltype, query, &sr)
	if err != nil {
		err = fmt.Errorf("(PutAcl) node=%s, acl=%s, user=%s: %s", nodeid, acltype, username, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(PutAcl) node=%s, acl=%s, user=%s: %s", nodeid, acltype, username, strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) DeleteAcl(nodeid string, acltype string, username string) (err error) {
	if (acltype == "") || (username == "") {
		return
	}
	var query url.Values
	query.Add("users", username)

	sr := new(ShockResponseGeneric)
	err = sc.deleteRequest("/node/"+nodeid+"/acl/"+acltype, query, &sr)
	if err != nil {
		err = fmt.Errorf("(DeleteAcl) node=%s, acl=%s, user=%s: %s", nodeid, acltype, username, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(DeleteAcl) node=%s, acl=%s, user=%s: %s", nodeid, acltype, username, strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) GetAcl(nodeid string) (sr *ShockResponseGeneric, err error) {
	sr = new(ShockResponseGeneric)
	err = sc.getRequest("/node/"+nodeid+"/acl", nil, &sr)
	if err != nil {
		err = fmt.Errorf("(GetAcl) node=%s: %s", nodeid, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(GetAcl) node=%s: %s", nodeid, strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) MakePublic(nodeid string) (err error) {
	sr := new(ShockResponseGeneric)
	err = sc.putRequest("/node/"+nodeid+"/acl/public_read", nil, &sr)
	if err != nil {
		err = fmt.Errorf("(MakePublic) node=%s: %s", nodeid, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(MakePublic) node=%s: %s", nodeid, strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) GetNodeDownloadUrl(node ShockNode) (downloadUrl string, err error) {
	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
		return
	}
	(*myurl).Path = fmt.Sprint("node/", node.Id)
	(*myurl).RawQuery = "download"
	downloadUrl = myurl.String()
	return
}

func (sc *ShockClient) Query(query url.Values) (sr *ShockQueryResponse, err error) {
	query.Add("query", "")
	sr = new(ShockQueryResponse)
	err = sc.getRequest("/node/", query, &sr)
	if err != nil {
		err = fmt.Errorf("(Query) %s", err.Error())
		return
	}
	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(Query) %s", strings.Join(sr.Errs, ","))
	}
	return
}

func (sc *ShockClient) QueryPaginated(resource string, query url.Values, limit int, offset int) (rc *httpclient.RestClient, err error) {
	query.Add("query", "")
	query.Set("limit", strconv.Itoa(limit))
	query.Set("offset", strconv.Itoa(offset))

	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
		err = fmt.Errorf("(QueryPaginated) %s", err.Error())
		return
	}
	(*myurl).Path = resource
	(*myurl).RawQuery = query.Encode()
	shockurl := myurl.String()

	rc = new(httpclient.RestClient)
	if sc.Token != "" {
		rc.User = httpclient.GetUserByTokenAuth(sc.Token)
	}
	if sc.Debug {
		fmt.Fprintf(os.Stdout, "QueryPaginated url: %s\n", shockurl)
	}
	rc.InitPagination(shockurl, "data", "offset")
	return
}

func (sc *ShockClient) GetNode(nodeid string) (node *ShockNode, err error) {
	sr := new(ShockResponse)
	err = sc.getRequest("/node/"+nodeid, nil, &sr)
	if err != nil {
		err = fmt.Errorf("(GetNode) node=%s: %s", nodeid, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(GetNode) node=%s: %s", nodeid, strings.Join(sr.Errs, ","))
		return
	}

	node = sr.Data
	if node == nil {
		err = fmt.Errorf("(GetNode) node=%s: empty node returned from Shock", nodeid)
	}
	return
}

func (sc *ShockClient) DeleteNode(nodeid string) (err error) {
	sr := new(ShockResponseGeneric)
	err = sc.deleteRequest("/node"+nodeid, nil, &sr)
	if err != nil {
		err = fmt.Errorf("(DeleteNode) node=%s: %s", nodeid, err.Error())
		return
	}

	if len(sr.Errs) > 0 {
		err = fmt.Errorf("(DeleteNode) node=%s: %s", nodeid, strings.Join(sr.Errs, ","))
	}
	return
}

// old-style functions that probably should to be refactored

func ShockGet(host string, nodeid string, token string) (node *ShockNode, err error) {
	if host == "" || nodeid == "" {
		err = errors.New("empty shock host or node id")
		return
	}
	sc := ShockClient{Host: host, Token: token}

	node, err = sc.GetNode(nodeid)
	return
}

func ShockDelete(host string, nodeid string, token string) (err error) {
	if host == "" || nodeid == "" {
		return errors.New("empty shock host or node id")
	}
	sc := ShockClient{Host: host, Token: token}

	err = sc.DeleteNode(nodeid)
	return
}

// fetch file by shock url
func FetchFile(filename string, url string, token string, uncompress string, computeMD5 bool) (size int64, md5sum string, err error) {
	var localfile *os.File
	localfile, err = os.Create(filename)
	if err != nil {
		return
	}
	defer localfile.Close()

	var body io.ReadCloser
	body, err = FetchShockStream(url, token)
	if err != nil {
		err = errors.New("(FetchFile) " + err.Error())
		return
	}
	defer body.Close()

	// set md5 compute
	md5h := md5.New()

	if uncompress == "" {
		// split stream to file and md5
		var dst io.Writer
		if computeMD5 {
			dst = io.MultiWriter(localfile, md5h)
		} else {
			dst = localfile
		}
		size, err = io.Copy(dst, body)
		if err != nil {
			return
		}
	} else if uncompress == "gzip" {
		// split stream to gzip and md5
		var input io.ReadCloser
		if computeMD5 {
			pReader, pWriter := io.Pipe()
			defer pReader.Close()
			dst := io.MultiWriter(pWriter, md5h)
			go func() {
				io.Copy(dst, body)
				pWriter.Close()
			}()
			input = pReader
		} else {
			input = body
		}

		gr, gerr := gzip.NewReader(input)
		if gerr != nil {
			err = gerr
			return
		}
		defer gr.Close()
		size, err = io.Copy(localfile, gr)
		if err != nil {
			return
		}
	} else {
		err = errors.New("(FetchFile) uncompress method unknown: " + uncompress)
		return
	}

	if computeMD5 {
		md5sum = fmt.Sprintf("%x", md5h.Sum(nil))
	}
	return
}

func FetchShockStream(url string, token string) (r io.ReadCloser, err error) {
	var user *httpclient.Auth
	if token != "" {
		user = httpclient.GetUserByTokenAuth(token)
	}

	//download file from Shock
	var res *http.Response
	res, err = httpclient.Get(url, httpclient.Header{}, user)
	if err != nil {
		err = errors.New("(FetchShockStream) httpclient.Get returned: " + err.Error())
		return
	}

	if res.StatusCode != 200 { //err in fetching data
		resbody, _ := ioutil.ReadAll(res.Body)
		err = errors.New(fmt.Sprintf("(FetchShockStream) url=%s, res=%s", url, resbody))
		return
	}

	r = res.Body
	return
}

// source:  http://stackoverflow.com/a/22259280
// TODO this is not shock related, need another package
func CopyFile(src, dst string) (size int64, err error) {
	var src_file *os.File
	src_file, err = os.Open(src)
	if err != nil {
		return
	}
	defer src_file.Close()

	var src_file_stat os.FileInfo
	src_file_stat, err = src_file.Stat()
	if err != nil {
		return
	}

	if !src_file_stat.Mode().IsRegular() {
		err = fmt.Errorf("%s is not a regular file", src)
		return
	}

	var dst_file *os.File
	dst_file, err = os.Create(dst)
	if err != nil {
		return
	}
	defer dst_file.Close()
	size, err = io.Copy(dst_file, src_file)
	return
}

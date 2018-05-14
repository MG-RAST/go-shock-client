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
	"strconv"
	"strings"
	"time"
)

// TODO use Token

var SHOCK_TIMEOUT = 60 * time.Second

type ShockClient struct {
	Host  string
	Token string
	Debug bool
}

type ShockResponse struct {
	Code int       `bson:"status" json:"status"`
	Data ShockNode `bson:"data" json:"data"`
	Errs []string  `bson:"error" json:"error"`
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
	Indexes      map[string]idxInfo `bson:"indexes" json:"indexes"`
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
}

type idxInfo struct {
	TotalUnits  int64     `bson:"total_units" json:"total_units"`
	AvgUnitSize int64     `bson:"average_unit_size" json:"average_unit_size"`
	CreatedOn   time.Time `bson:"created_on" json:"created_on"`
}

type linkage struct {
	Type      string   `bson: "relation" json:"relation"`
	Ids       []string `bson:"ids" json:"ids"`
	Operation string   `bson:"operation" json:"operation"`
}

type partsFile []string

type partsList struct {
	Count       int         `bson:"count" json:"count"`
	Length      int         `bson:"length" json:"length"`
	VarLen      bool        `bson:"varlen" json:"varlen"`
	Parts       []partsFile `bson:"parts" json:"parts"`
	Compression string      `bson:"compression" json:"compression"`
}

// *** low-level functions ***

func (sc *ShockClient) Post_request(resource string, query url.Values, response interface{}) (err error) {
	return sc.Do_request("POST", resource, query, response)
}

func (sc *ShockClient) Get_request(resource string, query url.Values, response interface{}) (err error) {
	return sc.Do_request("GET", resource, query, response)
}

func (sc *ShockClient) Put_request(resource string, query url.Values, response interface{}) (err error) {
	return sc.Do_request("PUT", resource, query, response)
}

func (sc *ShockClient) Do_request_string(method string, resource string, query url.Values) (jsonstream []byte, err error) {
	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
		return
	}

	(*myurl).Path = resource
	(*myurl).RawQuery = query.Encode()
	shockurl := myurl.String()

	if sc.Debug {
		fmt.Fprintf(os.Stdout, "Get_request url: %s\n", shockurl)
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

func (sc *ShockClient) Do_request(method string, resource string, query url.Values, response interface{}) (err error) {
	var jsonstream []byte
	jsonstream, err = sc.Do_request_string(method, resource, query)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonstream, response)
	return
}

// *** high-level functions ***

func (sc *ShockClient) Get_node_download_url(node ShockNode) (download_url string, err error) {
	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
		return
	}
	(*myurl).Path = fmt.Sprint("node/", node.Id)
	(*myurl).RawQuery = "download"
	download_url = myurl.String()
	return
}

func (sc *ShockClient) Make_public(node_id string) (sqr_p *ShockResponseGeneric, err error) {
	sqr_p = new(ShockResponseGeneric)
	err = sc.Put_request("/node/"+node_id+"/acl/public_read", nil, &sqr_p)
	return
}

func (sc *ShockClient) Query(query url.Values) (sqr_p *ShockQueryResponse, err error) {
	query.Add("query", "")
	sqr_p = new(ShockQueryResponse)
	err = sc.Get_request("/node/", query, &sqr_p)
	return
}

func (sc *ShockClient) QueryPaginated(resource string, query url.Values, limit int, offset int) (rc *httpclient.RestClient, err error) {
	query.Add("query", "")
	query.Set("limit", strconv.Itoa(limit))
	query.Set("offset", strconv.Itoa(offset))

	var myurl *url.URL
	myurl, err = url.ParseRequestURI(sc.Host)
	if err != nil {
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

func (sc *ShockClient) Get_node(node_id string) (sqr_p *ShockResponse, err error) {
	sqr_p = new(ShockResponse)
	err = sc.Get_request("/node/"+node_id, nil, &sqr_p)
	return
}

// old-style functions that probably should to be refactored

func ShockGet(host string, nodeid string, token string) (node *ShockNode, err error) {
	if host == "" || nodeid == "" {
		err = errors.New("empty shock host or node id")
		return
	}

	var res *http.Response
	shockurl := fmt.Sprintf("%s/node/%s", host, nodeid)

	var user *httpclient.Auth
	if token != "" {
		user = httpclient.GetUserByTokenAuth(token)
	}

	c := make(chan int, 1)
	go func() {
		res, err = httpclient.Get(shockurl, httpclient.Header{}, user)
		c <- 1 //we are ending
	}()
	select {
	case <-c:
	//go ahead
	case <-time.After(SHOCK_TIMEOUT):
		err = errors.New("timeout when getting node from shock, url=" + shockurl)
		return
	}
	if err != nil {
		return
	}
	defer res.Body.Close()

	var jsonstream []byte
	jsonstream, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	response := new(ShockResponse)
	if err = json.Unmarshal(jsonstream, response); err != nil {
		return
	}
	if len(response.Errs) > 0 {
		err = errors.New(strings.Join(response.Errs, ","))
		return
	}
	node = &response.Data
	if node == nil {
		err = errors.New("empty node got from Shock")
	}
	return
}

func ShockDelete(host string, nodeid string, token string) (err error) {
	if host == "" || nodeid == "" {
		return errors.New("empty shock host or node id")
	}

	var res *http.Response
	shockurl := fmt.Sprintf("%s/node/%s", host, nodeid)

	var user *httpclient.Auth
	if token != "" {
		user = httpclient.GetUserByTokenAuth(token)
	}

	c := make(chan int, 1)
	go func() {
		res, err = httpclient.Delete(shockurl, httpclient.Header{}, user)
		c <- 1 //we are ending
	}()
	select {
	case <-c:
	//go ahead
	case <-time.After(SHOCK_TIMEOUT):
		return errors.New("timeout when getting node from shock, url=" + shockurl)
	}
	if err != nil {
		return err
	}
	defer res.Body.Close()

	jsonstream, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	response := new(ShockResponse)
	if err := json.Unmarshal(jsonstream, response); err != nil {
		return err
	}
	if len(response.Errs) > 0 {
		return errors.New(strings.Join(response.Errs, ","))
	}
	return
}

//fetch file by shock url
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
		//logger.Debug(1, fmt.Sprintf("downloading file %s from %s", filename, url))
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
		//logger.Debug(1, fmt.Sprintf("downloading and unzipping file %s from %s", filename, url))
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

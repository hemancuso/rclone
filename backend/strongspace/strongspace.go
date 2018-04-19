// Package strongspace provides an interface to Google Cloud Storage
package strongspace

/*
Notes

Can't set Updated but can set Metadata on object creation

Patch needs full_control not just read_write

FIXME Patch/Delete/Get isn't working with files with spaces in - giving 404 error
- https://code.google.com/p/google-api-go-client/issues/detail?id=64
*/

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	_ "io/ioutil"
	"log"
	"net/http"
	_ "os"
	"path"
	"regexp"
	"strings"
	// "strconv"
	"sync"
	"time"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config"
	"github.com/ncw/rclone/fs/config/flags"
	"github.com/ncw/rclone/lib/pacer"
	"github.com/ncw/rclone/lib/rest"
	// "github.com/ncw/rclone/fs/config/obscure"
	_ "github.com/ncw/rclone/fs/fshttp"
	"github.com/ncw/rclone/fs/fserrors"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/fs/walk"
	"github.com/ncw/rclone/lib/oauthutil"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
_	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	storage "google.golang.org/api/storage/v1"
)

const (
	rcloneClientID              = "18fe3f557974546d993a569f357b449cdf199fbe35baad511e24e1a8546f7f1b"
	rcloneClientSecret = "5c91c2f18bc6aa51f3435f71bcac8c69e452bded441ca23d8c65224693328f69"
	timeFormatIn                = time.RFC3339
	timeFormatOut               = "2006-01-02T15:04:05.000000000Z07:00"
	minSleep                    = 10 * time.Millisecond
	maxSleep                    = 2 * time.Second
	decayConstant               = 2 
	rootURL                     = "http://expandrive.strongspace.net:8000/api/v2/files"
	metaMtime                   = "mtime" // key to store mtime under in metadata
	listChunks                  = 1000    // chunk size to read directory listings
)

var (
	gcsLocation     = flags.StringP("gcs-loc2ation", "", "", "Default location for buckets (us|eu|asia|us-central1|us-east1|us-east4|us-west1|asia-east1|asia-noetheast1|asia-southeast1|australia-southeast1|europe-west1|europe-west2).")
	gcsStorageClass = flags.StringP("gcs-stora2ge-class", "", "", "Default storage class for buckets (MULTI_REGIONAL|REGIONAL|STANDARD|NEARLINE|COLDLINE|DURABLE_REDUCED_AVAILABILITY).")
	// Description of how to auth for this app
	storageConfig = &oauth2.Config{
		Endpoint: oauth2.Endpoint{
			AuthURL:  "http://www2.strongspace.net:8000/oauth/authorize",
			TokenURL: "http://www2.strongspace.net:8000/oauth/token",
		},
		ClientID:     rcloneClientID,
		ClientSecret: rcloneClientSecret,
		RedirectURL:  oauthutil.TitleBarRedirectURL,
	}
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "strongspace",
		Description: "Strongspace",
		NewFs:       NewFs,
		Config: func(name string) {
			err := oauthutil.Config("strongspace", name, storageConfig)
			if err != nil {
				log.Fatalf("Failed to configure token: %v", err)
			}
		},
		Options: []fs.Option{{
			Name: "endpoint",
			Help: "Strongspace endpoint",
		}},
	})
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(resp *http.Response, err error) (bool, error) {
	authRety := false

	if resp != nil && resp.StatusCode == 401 && len(resp.Header["Www-Authenticate"]) == 1 && strings.Index(resp.Header["Www-Authenticate"][0], "expired_token") >= 0 {
		authRety = true
		fs.Debugf(nil, "Should retry: %v", err)
	}
	return authRety || fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}


// Fs represents a remote storage server
type Fs struct {
	name          string           // name of this remote
	root          string           // the path we are working on if any
	features      *fs.Features     // optional features
	svc           *storage.Service // the connection to the storage server
	pacer        *pacer.Pacer 
	srv          *rest.Client
	client        *http.Client     // authorized client
	bucket        string           // the bucket we are working on
	bucketOKMu    sync.Mutex       // mutex to protect bucket OK
	bucketOK      bool             // true if we have created the bucket
	projectNumber string           // used for finding buckets
	objectACL     string           // used when creating new objects
	bucketACL     string           // used when creating new buckets
	location      string           // location of new buckets
	storageClass  string           // storage class of new buckets
}

// Object describes a storage object
//
// Will definitely have info but maybe not meta
type Object struct {
	fs       *Fs       // what this object is part of
	remote   string    // The remote path
	url      string    // download path
	md5sum   string    // The MD5Sum of the object
	bytes    int64     // Bytes in the object
	modTime  time.Time // Modified time of the object
	mimeType string
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	if f.root == "" {
		return f.bucket
	}
	return f.bucket + "/" + f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.root == "" {
		return fmt.Sprintf("Storage bucket %s", f.bucket)
	}
	return fmt.Sprintf("Storage bucket %s path %s", f.bucket, f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Pattern to match a storage path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parseParse parses a storage 'url'
func parsePath(path string) (bucket, directory string, err error) {
	parts := matcher.FindStringSubmatch(path)
	if parts == nil {
		err = errors.Errorf("couldn't find bucket in storage path %q", path)
	} else {
		bucket, directory = parts[1], parts[2]
		directory = strings.Trim(directory, "/")
	}
	return
}


// NewFs contstructs an Fs from the path, bucket:path
func NewFs(name, root string) (fs.Fs, error) {
	var oAuthClient *http.Client
	var err error


	oAuthClient, _, err = oauthutil.NewClient(name, storageConfig)

	if err != nil {
		log.Fatalf("Failed to configure Google Cloud Storage: %v", err)
	}

	bucket, directory, err := parsePath(root)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:          name,
		bucket:        bucket,
		root:          directory,
		srv:         rest.NewClient(oAuthClient).SetRoot(rootURL),
		pacer:       pacer.New().SetMinSleep(minSleep).SetMaxSleep(maxSleep).SetDecayConstant(decayConstant),
		objectACL:     config.FileGet(name, "object_acl"),
		bucketACL:     config.FileGet(name, "bucket_acl"),
		location:      config.FileGet(name, "location"),
		storageClass:  config.FileGet(name, "storage_class"),
	}
	f.features = (&fs.Features{
		ReadMimeType:  true,
		WriteMimeType: true,
		BucketBased:   true,
	}).Fill(f)
	if f.objectACL == "" {
		f.objectACL = "private"
	}
	if f.bucketACL == "" {
		f.bucketACL = "private"
	}
	if *gcsLocation != "" {
		f.location = *gcsLocation
	}
	if *gcsStorageClass != "" {
		f.storageClass = *gcsStorageClass
	}

	// Create a new authorized Drive client.
	f.client = oAuthClient
	f.svc, err = storage.New(f.client)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create Google Cloud Storage client")
	}

	if f.root != "" {
		f.root += "/"
		// Check to see if the object exists
		_, err = f.svc.Objects.Get(bucket, directory).Do()
		if err == nil {
			f.root = path.Dir(directory)
			if f.root == "." {
				f.root = ""
			} else {
				f.root += "/"
			}
			// return an error with an fs which points to the parent
			return f, fs.ErrorIsFile
		}
	}
	return f, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(remote string, info *storage.Object) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		o.setMetaData(info)
	} else {
		err := o.readMetaData() // reads info and meta, returning an error
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	return f.newObjectWithInfo(remote, nil)
}

// listFn is called from list to handle an object.
type listFn func(remote string, object *storage.Object, isDirectory bool) error

type Time time.Time


type Item struct {
	Type              string `json:"type"`
	ID                string `json:"id"`
	SequenceID        string `json:"sequence_id"`
	Etag              string `json:"etag"`
	SHA1              string `json:"sha1"`
	Name              string `json:"name"`
	Size              int64  `json:"size"`
	Folder 	          bool   `json:"folder"`
	CreatedAt         Time   `json:"created_at"`
	ModifiedAt        int64  `json:"mtime"`
	ContentCreatedAt  Time   `json:"content_created_at"`
	ContentModifiedAt Time   `json:"content_modified_at"`
	ItemStatus        string `json:"item_status"` // active, trashed if the file has been moved to the trash, and deleted if the file has been permanently deleted
}

type FolderItems struct {
	TotalCount int    `json:"total_count"`
	Entries    []Item `json:"contents"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
	Order      []struct {
		By        string `json:"by"`
		Direction string `json:"direction"`
	} `json:"order"`
}


// list the objects into the function supplied
//
// dir is the starting directory, "" for root
//
// Set recurse to read sub directories
func (f *Fs) list(dir string, recurse bool, fn listFn) error {



	root := f.root
	// rootLength := len(root)


	if dir != "" {
		root += dir //+ "/"
	}

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/" + f.bucket + "/" + root,
	}


	var err error;

	var resp *http.Response
	var result FolderItems


	err = f.pacer.Call(func() (bool, error) {

		resp, err = f.srv.CallJSON(&opts, nil, &result)

		return shouldRetry(resp, err)
	})


	if err != nil {
		fmt.Println("err")
		fmt.Println(err)
		fmt.Println("that was it")
		// return found, errors.Wrap(err, "couldn't list files")
	}
	// fmt.Println(result)
//	for i := range result.Entries {

//		item := &result.Entries[i]

		// o, err := f.newObjectWithInfo("/" + f.bucket + "/" + root + "/", item)
		// fn("/" + f.bucket + "/" + root + "/" + item.Name, o, false)
	//}
		// 	item := &result.Entries[i]
		// 	if item.Type == api.ItemTypeFolder {
		// 		if filesOnly {
		// 			continue
		// 		}
		// 	} else if item.Type == api.ItemTypeFile {
		// 		if directoriesOnly {
		// 			continue
		// 		}
		// 	} else {
		// 		fs.Debugf(f, "Ignoring %q - unknown type %q", item.Name, item.Type)
		// 		continue
		// 	}
		// 	if item.ItemStatus != api.ItemStatusActive {
		// 		continue
		// 	}
		// 	item.Name = restoreReservedChars(item.Name)
		// 	if fn(item) {
		// 		found = true
		// 		break OUTER
		// 	}
		// }
		// offset += result.Limit
		// if offset >= result.TotalCount {
		// 	break
		// }
	// }
	// return

	// list := f.svc.Objects.List(f.bucket).Prefix(root).MaxResults(listChunks)
	// if !recurse {
	// 	list = list.Delimiter("/")
	// }
	// for {
	// 	objects, err := list.Do()
	// 	if err != nil {
	// 		if gErr, ok := err.(*googleapi.Error); ok {
	// 			if gErr.Code == http.StatusNotFound {
	// 				err = fs.ErrorDirNotFound
	// 			}
	// 		}
	// 		return err
	// 	}
	// 	if !recurse {
	// 		var object storage.Object
	// 		for _, prefix := range objects.Prefixes {
	// 			if !strings.HasSuffix(prefix, "/") {
	// 				continue
	// 			}
	// 			err = fn(prefix[rootLength:len(prefix)-1], &object, true)
	// 			if err != nil {
	// 				return err
	// 			}
	// 		}
	// 	}
	// 	for _, object := range objects.Items {
	// 		if !strings.HasPrefix(object.Name, root) {
	// 			fs.Logf(f, "Odd name received %q", object.Name)
	// 			continue
	// 		}
	// 		remote := object.Name[rootLength:]
	// 		// is this a directory marker?
	// 		if (strings.HasSuffix(remote, "/") || remote == "") && object.Size == 0 {
	// 			if recurse {
	// 				// add a directory in if --fast-list since will have no prefixes
	// 				err = fn(remote[:len(remote)-1], object, true)
	// 				if err != nil {
	// 					return err
	// 				}
	// 			}
	// 			continue // skip directory marker
	// 		}
	// 		err = fn(remote, object, false)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	if objects.NextPageToken == "" {
	// 		break
	// 	}
	// 	list.PageToken(objects.NextPageToken)
	// }
	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(remote string, object *storage.Object, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		d := fs.NewDir(remote, time.Time{}).SetSize(int64(object.Size))
		return d, nil
	}
	o, err := f.newObjectWithInfo(remote, object)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// mark the bucket as being OK
func (f *Fs) markBucketOK() {
	if f.bucket != "" {
		f.bucketOKMu.Lock()
		f.bucketOK = true
		f.bucketOKMu.Unlock()
	}
}

// listDir lists a single directory
func (f *Fs) listDir(dir string) (entries fs.DirEntries, err error) {
	// List the objects
	fmt.Println("listdir: " + f.bucket + "/" + dir)
	fmt.Println("list: " + dir + " - 0 ")


	root := f.root
	// rootLength := len(root)
	fmt.Println(f.bucket + "/" + root)

	if dir != "" {
		root += dir //+ "/"
	}

	opts := rest.Opts{
		Method:     "GET",
		Path:       "/" + f.bucket + "/" + root,
	}




	var resp *http.Response
	var result FolderItems


	err = f.pacer.Call(func() (bool, error) {

		resp, err = f.srv.CallJSON(&opts, nil, &result)

		return shouldRetry(resp, err)
	})


	if err != nil {
		fmt.Println("err")
		fmt.Println(err)
		// return found, errors.Wrap(err, "couldn't list files")
	}

	for i := range result.Entries {
		item := &result.Entries[i]


		// f.bucket + "/" + root +

		remote :=  item.Name
		if len(dir) > 0 {
			remote = dir + "/" + item.Name
		}
		entry := &Object{
			fs:     f,
			remote:  remote,
			bytes: item.Size,
			modTime: time.Unix(item.ModifiedAt, 0),
			url: rootURL + "/" + f.bucket + "/" + f.root + remote,
		}


		// if info != nil {
		// 	o.setMetaData(info)
		// } else {
		// 	err := o.readMetaData() // reads info and meta, returning an error
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// }

		// entry, err := f.itemToDirEntry(remote, item, false)

		if item.Folder == true {
			entries = append(entries, fs.NewDir(remote, time.Unix(item.ModifiedAt, 0)))
		} else if entry != nil {
			entries = append(entries, entry)
		}
		// o, err := f.newObjectWithInfo("/" + f.bucket + "/" + root + "/", item)
		// fn("/" + f.bucket + "/" + root + "/" + item.Name, o, false)
	}

	// err = f.list(dir, false, func(remote string, object *storage.Object, isDirectory bool) error {
	// 	entry, err := f.itemToDirEntry(remote, object, isDirectory)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if entry != nil {
	// 		entries = append(entries, entry)
	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	return nil, err
	// }
	// bucket must be present if listing succeeded
	f.markBucketOK()
	return entries, err
}

// listBuckets lists the buckets
func (f *Fs) listBuckets(dir string) (entries fs.DirEntries, err error) {
	fmt.Println("list buckets")
	fmt.Println(dir)
	
	if dir != "" {
		return nil, fs.ErrorListBucketRequired
	}
	

	d := fs.NewDir("expandrive", time.Time{})
	entries = append(entries, d)

	// listBuckets := f.svc.Buckets.List(f.projectNumber).MaxResults(listChunks)
	// for {
	// 	buckets, err := listBuckets.Do()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	for _, bucket := range buckets.Items {
	// 		d := fs.NewDir(bucket.Name, time.Time{})
	// 		entries = append(entries, d)
	// 	}
	// 	if buckets.NextPageToken == "" {
	// 		break
	// 	}
	// 	listBuckets.PageToken(buckets.NextPageToken)
	// }
	return entries, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {

	if f.bucket == "" {
		return f.listBuckets(dir)
	}
	return f.listDir(dir)
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	if f.bucket == "" {
		return fs.ErrorListBucketRequired
	}
	list := walk.NewListRHelper(callback)
	err = f.list(dir, true, func(remote string, object *storage.Object, isDirectory bool) error {
		entry, err := f.itemToDirEntry(remote, object, isDirectory)
		if err != nil {
			return err
		}
		return list.Add(entry)
	})
	if err != nil {
		return err
	}
	// bucket must be present if listing succeeded
	f.markBucketOK()
	return list.Flush()
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {

	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {

	return f.Put(in, src, options...)
}

// Mkdir creates the bucket if it doesn't exist
func (f *Fs) Mkdir(dir string) error {
	fmt.Println("MKDIR " + dir)
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.bucketOK {
		return nil
	}
	_, err := f.svc.Buckets.Get(f.bucket).Do()
	if err == nil {
		// Bucket already exists
		f.bucketOK = true
		return nil
	} else if gErr, ok := err.(*googleapi.Error); ok {
		if gErr.Code != http.StatusNotFound {
			return errors.Wrap(err, "failed to get bucket")
		}
	} else {
		return errors.Wrap(err, "failed to get bucket")
	}

	if f.projectNumber == "" {
		return errors.New("can't make bucket without project number")
	}

	bucket := storage.Bucket{
		Name:         f.bucket,
		Location:     f.location,
		StorageClass: f.storageClass,
	}
	_, err = f.svc.Buckets.Insert(f.projectNumber, &bucket).PredefinedAcl(f.bucketACL).Do()
	if err == nil {
		f.bucketOK = true
	}
	return err
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty: Error 409: The bucket you tried
// to delete was not empty.
func (f *Fs) Rmdir(dir string) error {
	f.bucketOKMu.Lock()
	defer f.bucketOKMu.Unlock()
	if f.root != "" || dir != "" {
		return nil
	}
	err := f.svc.Buckets.Delete(f.bucket).Do()
	if err == nil {
		f.bucketOK = false
	}
	return err
}

// Precision returns the precision
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	err := f.Mkdir("")
	if err != nil {
		return nil, err
	}
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}

	// Temporary Object under construction
	dstObj := &Object{
		fs:     f,
		remote: remote,
	}

	srcBucket := srcObj.fs.bucket
	srcObject := srcObj.fs.root + srcObj.remote
	dstBucket := f.bucket
	dstObject := f.root + remote
	newObject, err := f.svc.Objects.Copy(srcBucket, srcObject, dstBucket, dstObject, nil).Do()
	if err != nil {
		return nil, err
	}
	// Set the metadata for the new object while we have it
	dstObj.setMetaData(newObject)
	return dstObj, nil
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5sum, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.bytes
}

// setMetaData sets the fs data from a storage.Object
func (o *Object) setMetaData(info *storage.Object) {
	o.url = info.MediaLink
	o.bytes = int64(info.Size)
	o.mimeType = info.ContentType

	// Read md5sum
	md5sumData, err := base64.StdEncoding.DecodeString(info.Md5Hash)
	if err != nil {
		fs.Logf(o, "Bad MD5 decode: %v", err)
	} else {
		o.md5sum = hex.EncodeToString(md5sumData)
	}

	// read mtime out of metadata if available
	mtimeString, ok := info.Metadata[metaMtime]
	if ok {
		modTime, err := time.Parse(timeFormatIn, mtimeString)
		if err == nil {
			o.modTime = modTime
			return
		}
		fs.Debugf(o, "Failed to read mtime from metadata: %s", err)
	}

	// Fallback to the Updated time
	modTime, err := time.Parse(timeFormatIn, info.Updated)
	if err != nil {
		fs.Logf(o, "Bad time decode: %v", err)
	} else {
		o.modTime = modTime
	}
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData() (err error) {

	if !o.modTime.IsZero() {
		return nil
	}
	object, err := o.fs.svc.Objects.Get(o.fs.bucket, o.fs.root+o.remote).Do()
	if err != nil {
		if gErr, ok := err.(*googleapi.Error); ok {
			if gErr.Code == http.StatusNotFound {
				return fs.ErrorObjectNotFound
			}
		}
		return err
	}
	o.setMetaData(object)
	return nil
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	err := o.readMetaData()
	if err != nil {
		// fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	return o.modTime
}

// Returns metadata for an object
func metadataFromModTime(modTime time.Time) map[string]string {
	metadata := make(map[string]string, 1)
	metadata[metaMtime] = modTime.Format(timeFormatOut)
	return metadata
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) error {
	// This only adds metadata so will perserve other metadata
	object := storage.Object{
		Bucket:   o.fs.bucket,
		Name:     o.fs.root + o.remote,
		Metadata: metadataFromModTime(modTime),
	}
	newObject, err := o.fs.svc.Objects.Patch(o.fs.bucket, o.fs.root+o.remote, &object).Do()
	if err != nil {
		return err
	}
	o.setMetaData(newObject)
	return nil
}

// Storable returns a boolean as to whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fmt.Println("open")
	fmt.Println(o.url)
	req, err := http.NewRequest("GET", o.url, nil)
	if err != nil {
		return nil, err
	}
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	res, err := o.fs.client.Do(req)
	if err != nil {
		return nil, err
	}
	_, isRanging := req.Header["Range"]
	if !(res.StatusCode == http.StatusOK || (isRanging && res.StatusCode == http.StatusPartialContent)) {
		_ = res.Body.Close() // ignore error
		return nil, errors.Errorf("bad response: %d: %s", res.StatusCode, res.Status)
	}
	return res.Body, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {



	t := fmt.Sprintf("%d", src.ModTime().Unix())



	opts := rest.Opts{
		Method:     "POST",
		Body:   in,
		Path:       "/" + o.fs.bucket + "/" + o.fs.root + o.remote,
		MultipartFileName:     "jeff",
		ExtraHeaders: map[string]string{"attributes": "{\"modified_at\": " + t  + "}"}, 
	}


	var err error;

	var resp *http.Response
	var result FolderItems



	err = o.fs.pacer.Call(func() (bool, error) {

		resp, err = o.fs.srv.CallJSON(&opts, nil, &result)

		return shouldRetry(resp, err)
	})
	o.bytes = src.Size()

	if err != nil {
		
		// o.size = src.Size
		// return found, errors.Wrap(err, "couldn't list files")
	}

	// upload := api.UploadFile{
	// 	Name:              replaceReservedChars(leaf),
	// 	ContentModifiedAt: api.Time(modTime),
	// 	ContentCreatedAt:  api.Time(modTime),
	// 	Parent: api.Parent{
	// 		ID: directoryID,
	// 	},
	// }

	// var resp *http.Response
	// var result FolderItems
	// opts := rest.Opts{
	// 	Method: "POST",
	// 	Body:   in,
	// 	MultipartMetadataName: "attributes",
	// 	MultipartContentName:  "contents",
	// }
	// // If object has an ID then it is existing so create a new version
	// if o.id != "" {
	// 	opts.Path = "/files/" + o.id + "/content"
	// } else {
	// 	opts.Path = "/files/content"
	// }
	// err = o.fs.pacer.CallNoRetry(func() (bool, error) {
	// 	resp, err = o.fs.srv.CallJSON(&opts, &upload, &result)
	// 	return shouldRetry(resp, err)
	// })
	// if err != nil {
	// 	return err
	// }
	// if result.TotalCount != 1 || len(result.Entries) != 1 {
	// 	return errors.Errorf("failed to upload %v - not sure why", o)
	// }
	// return o.setMetaData(&result.Entries[0])

	return nil
}
	// Set the metadata for the new object while we have it
// 	o.setMetaData(newObject)
// 	return nil
// }

// Remove an object
func (o *Object) Remove() error {
	return o.fs.svc.Objects.Delete(o.fs.bucket, o.fs.root+o.remote).Do()
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType() string {
	return o.mimeType
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.Copier      = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.ListRer     = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
)

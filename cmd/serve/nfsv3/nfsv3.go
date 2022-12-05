// Package nfsv3 provides common functionality for nfsv3 servers
package nfsv3

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v5"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
	"github.com/spf13/cobra"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

const separator = filepath.Separator

// Options required for nfsv3 server
type Options struct {
}

// DefaultOpt is the default values used for Options
var DefaultOpt = Options{}

// Opt is options set by command line flags
var Opt = DefaultOpt

func init() {
	vfsflags.AddFlags(Command.Flags())
}

// Command definition for cobra
var Command = &cobra.Command{
	Use:   "nfsv3 remote:path",
	Short: `Serve the remote over NFS v3.`,
	Long: `Run a basic nfs v3 server to serve a remote over NFS version 3 (TCP).
You can use the filter flags (e.g. ` + "`--include`, `--exclude`" + `) to control what
is served.

The server will log errors.  Use ` + "`-v`" + ` to see access logs.

` + "`--bwlimit`" + ` will be respected for file transfers.  Use ` + "`--stats`" + ` to
control the stats printing.
` + vfs.Help,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsSrc(args)
		cmd.Run(false, true, command, func() error {
			listener, err := net.Listen("tcp", ":2049")
			if err != nil {
				return err
			}

			s := newServer(f)
			return nfs.Serve(listener, s.handler)
		})
	},
}

type billyFs struct {
	// billy.Basic
	// billy.Dir
	*vfs.VFS
}

func (billyFs) Capabilities() billy.Capability {
	// return billy.DefaultCapabilities & ^billy.LockCapability
	return billy.ReadCapability | billy.SeekCapability
}

type billyFile struct {
	// billy.File
	vfs.Handle
}

// billy.Basic

func (fs billyFs) Create(filename string) (billy.File, error) {
	handle, err := fs.VFS.Create(filename)
	return billyFile{handle}, err
}

func (fs billyFs) Open(filename string) (billy.File, error) {
	handle, err := fs.VFS.Open(filename)
	return billyFile{handle}, err
}

func (fs billyFs) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	handle, err := fs.VFS.OpenFile(filename, flag, perm)
	return billyFile{handle}, err
}

/*
type fileInfo struct {
	os.FileInfo
}

func (fi fileInfo) Name() string {
	name := fi.FileInfo.Name()
	return name[:len(name)-1]
}
*/

func (fs billyFs) Stat(filename string) (fi os.FileInfo, err error) {
	fi, err = fs.VFS.Stat(filename)
	fmt.Printf("Stat: %v  %T: %+v\n", filename, fi, fi.Name())
	return fi, err
}

func (fs billyFs) Join(elem ...string) string {
	if len(elem) == 0 {
		return "/"
	}
	return filepath.Join(elem...)
}

func (f billyFile) Lock() error {
	return billy.ErrNotSupported
}

func (f billyFile) Unlock() error {
	return billy.ErrNotSupported
}

// billy.Dir
func (fs billyFs) ReadDir(path string) (fis []os.FileInfo, err error) {
	return fs.VFS.ReadDir(path)
	/*
		// fmt.Printf("Dir: %+v (%+v)\n", path, err)
		for i, fi := range fis {
			tfi := fileInfo{fi}
			fis[i] = tfi
			fmt.Printf("    %T: %+v\n", tfi, tfi.Name())
			return fis, err
		}
		return fis, err
	*/
}

func (fs billyFs) MkdirAll(filename string, perm os.FileMode) error {
	return billy.ErrNotSupported
}

// billy.Chroot
func (fs billyFs) Chroot(path string) (billy.Filesystem, error) {
	return nil, billy.ErrNotSupported
}

func (fs billyFs) Root() string {
	return "/"
}

// billy.Symlink
func (fs billyFs) Lstat(filename string) (os.FileInfo, error) {
	return nil, billy.ErrNotSupported
}

func (fs billyFs) Symlink(target, link string) error {
	return billy.ErrNotSupported
}

func (fs billyFs) Readlink(link string) (string, error) {
	return "", billy.ErrNotSupported
}

// billy.Tempfile

func (fs billyFs) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}

// server contains everything to run the server
type server struct {
	handler nfs.Handler
}

func newServer(fs fs.Fs) *server {
	vfs := vfs.New(fs, &vfsflags.Opt)

	billyFs := billyFs{vfs}
	authHandler := nfshelper.NewNullAuthHandler(billyFs)
	cacheHelper := nfshelper.NewCachingHandler(authHandler, 65535)

	s := &server{
		handler: cacheHelper,
	}
	return s
}

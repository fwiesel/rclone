// Package nfsv3 provides common functionality for nfsv3 servers
package nfsv3

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	iofs "io/fs"
	"math"
	"net"
	"os"
	"path/filepath"

	billy "github.com/go-git/go-billy/v5"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
	"github.com/spf13/cobra"
	nfs "github.com/willscott/go-nfs"
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
` + vfs.Help,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsSrc(args)
		cmd.Run(false, true, command, func() error {
			listener, err := net.Listen("tcp4", ":2049")
			if err != nil {
				return err
			}

			s := newServer(f)
			return nfs.Serve(listener, s)
		})
	},
}

type billyFs struct {
	// billy.Filesystem
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

func (fs billyFs) Remove(filename string) error {
	return fs.VFS.Remove(filename)
}

func (fs billyFs) Rename(oldpath, newpath string) error {
	return fs.VFS.Rename(oldpath, newpath)
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
		return fs.Root()
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

type FileInfos []iofs.FileInfo

type VerifierPathPair struct {
	verifier uint64
	path     string
}

const MAX_FILE_HANDLES = 1024

// server contains everything to run the server
type server struct {
	// handler        nfs.Handler
	// cachingHandler nfs.CachingHandler
	fs                  billyFs
	pathForHandle       *lru.TwoQueueCache
	contentsForVerifier *lru.TwoQueueCache
}

func (s *server) Mount(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return nfs.MountStatusOk, s.fs, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (s *server) Change(fs billy.Filesystem) billy.Change {
	if c, ok := fs.(billy.Change); ok {
		return c
	}
	return nil
}

func (s *server) FSStat(ctx context.Context, fs billy.Filesystem, fsStat *nfs.FSStat) error {
	total, _, free := s.fs.VFS.Statfs()
	fsStat.TotalSize = uint64(total)
	fsStat.FreeSize = uint64(free)
	fsStat.AvailableSize = fsStat.FreeSize
	fsStat.TotalFiles = 0
	fsStat.FreeFiles = math.MaxUint64
	fsStat.AvailableFiles = math.MaxUint64
	return nil
}

func (s *server) ToHandle(fs billy.Filesystem, path []string) []byte {
	// up to 64bytes
	vHash := sha256.New()

	for _, item := range path {
		vHash.Write([]byte(item))
	}
	sum := vHash.Sum(nil)

	var key [32]byte
	copy(key[:], sum)
	s.pathForHandle.Add(key, path)
	return sum
}

func (s *server) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	var key [32]byte
	copy(key[:], fh)
	if value, ok := s.pathForHandle.Get(key); ok {
		if path, ok := value.([]string); ok {
			return s.fs, path, nil
		} else {
			return s.fs, nil, errors.New("invalid value in map")
		}
	}

	return s.fs, nil, errors.New("handle unknown or expired")
}

func (s *server) HandleLimit() int {
	return MAX_FILE_HANDLES
}

func (s *server) VerifierFor(path string, contents []iofs.FileInfo) uint64 {
	node, err := s.fs.VFS.Stat(path)
	if err != nil {
		return 0 // Will cause a retry of the read-dir plus
	}

	verifier := uint64(node.ModTime().UnixMicro())
	key := VerifierPathPair{verifier, path}
	s.contentsForVerifier.Add(key, contents)

	return verifier
}

func (s *server) DataForVerifier(path string, verifier uint64) []iofs.FileInfo {
	key := VerifierPathPair{verifier, path}
	if value, ok := s.contentsForVerifier.Get(key); ok {
		if fileInfos, ok := value.([]iofs.FileInfo); ok {
			return fileInfos
		}
	}

	return nil
}

func newServer(fs fs.Fs) *server {
	vfs := vfs.New(fs, &vfsflags.Opt)

	billyFs := billyFs{vfs}
	handleCache, _ := lru.New2Q(MAX_FILE_HANDLES)
	verifierCache, _ := lru.New2Q(MAX_FILE_HANDLES)
	s := &server{billyFs, handleCache, verifierCache}
	return s
}

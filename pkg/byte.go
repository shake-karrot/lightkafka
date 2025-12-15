package pkg

import "unsafe"

/* Find the pointer of the first element of the byte slice */
func ToUintptr(b []byte) uintptr {
	return uintptr(unsafe.Pointer(&b[0]))
}

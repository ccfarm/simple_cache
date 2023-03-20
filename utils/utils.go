package utils

import "unsafe"

func StringTobyteSlice(s string) []byte {
	tmp1 := (*[2]uintptr)(unsafe.Pointer(&s))
	tmp2 := [3]uintptr{tmp1[0], tmp1[1], tmp1[1]}

	return *(*[]byte)(unsafe.Pointer(&tmp2))

}

func ByteSliceToString(bytes []byte) string {

	return *(*string)(unsafe.Pointer(&bytes))

}

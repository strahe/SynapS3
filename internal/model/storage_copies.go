package model

const (
	StorageCopiesMin = 1
	StorageCopiesMax = 8
)

func ValidStorageCopies(copies int) bool {
	return copies >= StorageCopiesMin && copies <= StorageCopiesMax
}

func ClampStorageCopies(copies int) int {
	if copies < StorageCopiesMin {
		return StorageCopiesMin
	}
	if copies > StorageCopiesMax {
		return StorageCopiesMax
	}
	return copies
}

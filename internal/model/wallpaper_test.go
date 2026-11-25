package model

import "testing"

func TestCalcOrientation(t *testing.T) {
	tests := []struct {
		name   string
		width  uint32
		height uint32
		want   uint8
	}{
		{name: "standard landscape 16:9", width: 1920, height: 1080, want: OrientationLandscape},
		{name: "just above 1.05 threshold", width: 1051, height: 1000, want: OrientationLandscape},
		{name: "exact 1.05 upper boundary is square", width: 1050, height: 1000, want: OrientationSquare},
		{name: "exact 1:1 square", width: 2000, height: 2000, want: OrientationSquare},
		{name: "exact 0.95 lower boundary is square", width: 950, height: 1000, want: OrientationSquare},
		{name: "just below 0.95 threshold", width: 949, height: 1000, want: OrientationPortrait},
		{name: "standard portrait 9:16", width: 1080, height: 1920, want: OrientationPortrait},
		{name: "zero guard", width: 0, height: 1000, want: OrientationSquare},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CalcOrientation(tc.width, tc.height); got != tc.want {
				t.Fatalf("CalcOrientation(%d, %d) = %d, want %d", tc.width, tc.height, got, tc.want)
			}
		})
	}
}

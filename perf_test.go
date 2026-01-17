package fcache

import (
	"bytes"
	crypto "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"testing"
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = crypto.Read(b)
	return b
}

func BenchmarkCache_Put_SameKey(b *testing.B) {
	dir := b.TempDir()
	cache, err := Builder(dir, 1*GiB).Build()
	if err != nil {
		b.Fatal(err)
	}

	size := 7 * KiB
	key := uint64(1)
	data := randomBytes(int(size))

	b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := cache.Put(key, data, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCache_Get_SameKey(b *testing.B) {
	dir := b.TempDir()
	cache, err := Builder(dir, 1*GiB).Build()
	if err != nil {
		b.Fatal(err)
	}

	size := 7 * KiB
	key := uint64(1)
	data := randomBytes(int(size))

	_, err = cache.Put(key, data, 0)
	if err != nil {
		b.Fatal(err)
	}

	b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := cache.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCache_GetOrPut_FreshKey(b *testing.B) {
	dir := b.TempDir()
	cache, err := Builder(dir, 1*GiB).Build()
	if err != nil {
		b.Fatal(err)
	}

	size := 7 * KiB
	key := uint64(1)
	data := randomBytes(int(size))

	_, err = cache.Put(key, data, 0)
	if err != nil {
		b.Fatal(err)
	}

	b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _, err := cache.GetOrPut(uint64(i), 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
				written, err = io.Copy(sink, bytes.NewReader(data))
				return
			}))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func runWorkers(b *testing.B, cache Cache, items int64, size Size, workers int, work func(cache Cache, items int64, size Size, workers int, workerId int) error) {
	wgDone := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wgDone.Add(1)
		go func(i int) {
			defer wgDone.Done()
			err := work(cache, items, size, workers, i)
			if err != nil {
				b.Error("Worker", i, "failed:", err)
				return
			}
		}(i)
	}

	wgDone.Wait()
}

func putFreshKeyWorker(cache Cache, items int64, size Size, workers int, workerId int) error {
	for i := int64(workerId); i < items; i += int64(workers) {
		_, err := cache.PutReader(uint64(i), io.LimitReader(crypto.Reader, int64(size)), 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCache_Parallel_Put_FreshKey(b *testing.B) {
	items := int64(10000)
	size := 5 * KiB
	targetSize := size * Size(items)
	workers := 5

	b.Run(fmt.Sprintf("%d_items_%d_bytes_%d_workers", items, size, workers), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			runWorkers(b, cache, items, size, workers, putFreshKeyWorker)
		}
	})
}

func getFreshKeyWorker(cache Cache, items int64, _ Size, workers int, workerId int) error {
	for i := int64(workerId); i < items; i += int64(workers) {
		_, _, err := cache.Get(uint64(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCache_Parallel_Get_FreshKey(b *testing.B) {
	items := int64(10000)
	size := 5 * KiB
	targetSize := size * Size(items)
	workers := 5

	b.Run(fmt.Sprintf("%d_items_%d_bytes_%d_workers", items, size, workers), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			runWorkers(b, cache, items, size, 2, putFreshKeyWorker)
			b.StartTimer()
			runWorkers(b, cache, items, size, workers, getFreshKeyWorker)
		}
	})
}

func putRandomKeyWorker(cache Cache, items int64, size Size, _ int, workerId int) error {
	keyRand := rand.New(rand.NewPCG(uint64(items), uint64(workerId)))

	for i := int64(0); i < items; i++ {
		key := keyRand.Uint64N(uint64(items))
		_, err := cache.PutReader(key, io.LimitReader(crypto.Reader, int64(size)), 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCache_Parallel_Put_RandomKey(b *testing.B) {
	items := int64(10000)
	size := 5 * KiB
	targetSize := size * Size(items)
	workers := 5

	b.Run(fmt.Sprintf("%d_items_%d_bytes_%d_workers", items, size, workers), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			runWorkers(b, cache, items, size, workers, putRandomKeyWorker)
		}
	})
}

func getRandomKeyWorker(cache Cache, items int64, _ Size, _ int, workerId int) error {
	keyRand := rand.New(rand.NewPCG(uint64(items), uint64(workerId)))

	for i := int64(0); i < items; i++ {
		key := keyRand.Uint64N(uint64(items))
		_, _, err := cache.Get(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCache_Parallel_Get_RandomKey(b *testing.B) {
	items := int64(10000)
	size := 5 * KiB
	targetSize := size * Size(items)
	workers := 5

	b.Run(fmt.Sprintf("%d_items_%d_bytes_%d_workers", items, size, workers), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			runWorkers(b, cache, items, size, 2, putFreshKeyWorker)
			b.StartTimer()
			runWorkers(b, cache, items, size, workers, getRandomKeyWorker)
		}
	})
}

func randomLoadWorker(cache Cache, items int64, size Size, _ int, workerId int) error {
	keyRand := rand.New(rand.NewPCG(uint64(items), uint64(workerId)))
	actionRand := rand.New(rand.NewPCG(uint64(items), uint64(workerId)))

	for i := int64(0); i < items*3; i++ {
		key := keyRand.Uint64N(uint64(items))
		var err error
		switch actionRand.IntN(2) {
		case 0:
			_, err = cache.Delete(key)
		case 1:
			_, _, _, err = cache.GetOrPut(key, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
				return io.Copy(sink, io.LimitReader(crypto.Reader, int64(size)))
			}))
		default:
			panic("Unexpected action")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkCache_Parallel_Random_Load(b *testing.B) {
	items := int64(10)
	size := 5 * KiB
	targetSize := size * Size(items)
	workers := 50

	b.Run(fmt.Sprintf("%d_items_%d_bytes_%d_workers", items, size, workers), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			runWorkers(b, cache, items, size, workers, randomLoadWorker)
		}
	})
}

func writeMemoryUsage(w io.Writer, header bool) {
	if header {
		_, err := w.Write([]byte("TotalAlloc,Sys,HeapAlloc,HeapInuse,HeapSys,StackInuse,StackSys,NumGC\n"))
		if err != nil {
			panic("Failed to write memory header: " + err.Error())
		}
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	toMiB := func(b uint64) float32 { return float32(b) / 1024.0 / 1024.0 }

	line := fmt.Sprintf("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n",
		toMiB(m.TotalAlloc),
		toMiB(m.Sys),
		toMiB(m.HeapAlloc),
		toMiB(m.HeapInuse),
		toMiB(m.HeapSys),
		toMiB(m.StackInuse),
		toMiB(m.StackSys),
		m.NumGC,
	)
	_, err := w.Write([]byte(line))
	if err != nil {
		panic("Failed to write memory line: " + err.Error())
	}
}

func BenchmarkCache_MemoryUsage(b *testing.B) {
	items := int64(100000)
	size := 1 * KiB
	targetSize := size * Size(items)

	err := os.Mkdir("benchmarks", 0770)
	if err != nil && !errors.Is(err, os.ErrExist) {
		b.Fatal(err)
	}
	f, err := os.Create("benchmarks/memory.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	writeMemoryUsage(f, true)

	b.Run(fmt.Sprintf("%d_items_%d_bytes", items, size), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dir := b.TempDir()
			cache, err := Builder(dir, targetSize).Build()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			runWorkers(b, cache, items, size, 2, putFreshKeyWorker)
			b.StopTimer()
			runtime.GC()
			writeMemoryUsage(f, false)
			b.StartTimer()
		}
	})
}

func BenchmarkCache_loadEntries(b *testing.B) {
	items := int64(10000)
	size := 1 * Byte
	targetSize := size * Size(items)
	workers := 5

	dir := b.TempDir()
	cache, err := Builder(dir, targetSize).Build()
	if err != nil {
		b.Fatal(err)
	}
	runWorkers(b, cache, items, size, workers, putFreshKeyWorker)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := Builder(dir, targetSize).Build()
		if err != nil {
			b.Fatal(err)
		}
	}
}

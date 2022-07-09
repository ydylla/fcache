package fcache

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
)

func BenchmarkCache_Put_SameKey(b *testing.B) {
	dir := b.TempDir()
	cache, err := Builder(dir, 1*GiB).Build()
	if err != nil {
		b.Fatal(err)
	}

	size := 7 * KiB
	key := uint64(1)
	data := make([]byte, size)
	rand.Read(data)

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
	data := make([]byte, size)
	rand.Read(data)

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
	dataRand := rand.New(rand.NewSource(int64(workerId)))
	for i := int64(workerId); i < items; i += int64(workers) {
		_, err := cache.PutReader(uint64(i), io.LimitReader(dataRand, int64(size)), 0)
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
	keyRand := rand.New(rand.NewSource(int64(workerId)))
	dataRand := rand.New(rand.NewSource(int64(workerId)))

	for i := int64(0); i < items; i++ {
		key := keyRand.Int63n(items)
		_, err := cache.PutReader(uint64(key), io.LimitReader(dataRand, int64(size)), 0)
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
	keyRand := rand.New(rand.NewSource(int64(workerId)))

	for i := int64(0); i < items; i++ {
		key := keyRand.Int63n(items)
		_, _, err := cache.Get(uint64(key))
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
	keyRand := rand.New(rand.NewSource(int64(workerId)))
	dataRand := rand.New(rand.NewSource(int64(workerId)))
	actionRand := rand.New(rand.NewSource(int64(workerId)))

	for i := int64(0); i < items*3; i++ {
		key := uint64(keyRand.Int63n(items))
		var err error
		switch actionRand.Intn(2) {
		case 0:
			_, err = cache.Delete(key)
		case 1:
			_, _, _, err = cache.GetOrPut(key, 0, FillerFunc(func(key uint64, sink io.Writer) (written int64, err error) {
				return io.Copy(sink, io.LimitReader(dataRand, int64(size)))
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

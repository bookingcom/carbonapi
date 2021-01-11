package prioritylimiter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestSimple(t *testing.T) {
	limiter := New(3)
	ctx := context.TODO()

	err := limiter.Enter(ctx, 1, "1")
	if err != nil {
		t.Fatal(err)
	}

	err = limiter.Enter(ctx, 1, "1")
	if err != nil {
		t.Fatal(err)
	}
	if limiter.Active() != 2 {
		t.Fatal("active")
	}

	err = limiter.Leave()
	if err != nil {
		t.Fatal(err)
	}

	if limiter.Active() != 1 {
		t.Fatal("active")
	}

	err = limiter.Leave()
	if err != nil {
		t.Fatal(err)
	}

	if limiter.Active() != 0 {
		t.Fatal("active")
	}
}

func TestFail(t *testing.T) {
	limiter := New(2)
	ctx := context.TODO()

	err := limiter.Enter(ctx, 1, "1")
	if err != nil {
		t.Fatal(err)
	}

	if limiter.Active() != 1 {
		t.Fatal("active")
	}

	err = limiter.Leave()
	if err != nil {
		t.Fatal("Err is nil")
	}
	if limiter.Active() != 0 {
		t.Fatal("active")
	}

	err = limiter.Leave()
	if err == nil {
		t.Fatal("Err is nil")
	}
}

func TestCancelActive(t *testing.T) {
	limiter := New(2)
	ctx, cancel := context.WithCancel(context.Background())
	err := limiter.Enter(ctx, 1, "1")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if limiter.Active() != 1 {
		t.Fatal("active")
	}
	err = limiter.Leave()
	if err != nil {
		t.Fatal(err)
	}
	if limiter.Active() != 0 {		
		t.Fatal("active")
	}
}

func TestCancelBefore(t *testing.T) {
	limiter := New(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := limiter.Enter(ctx, 1, "1")
	if err == nil {
		t.Fatal("err")
	}
	limiter.waitLoopCount(1)
	if limiter.Active() != 0 {
		// we can either process the cancel(1 op) or the create request first(3 ops)
		limiter.waitLoopCount(3)
		if limiter.Active() != 0 {
			t.Fatal("active")
		}
	}
}

func TestCancelWaiting(t *testing.T) {
	limiter := New(1)

	err := limiter.Enter(context.TODO(), 1, "1")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go limiter.Enter(ctx, 1, "1")
	limiter.waitLoopCount(3)

	cancel()

	limiter.waitLoopCount(4)

	err = limiter.Leave()
	if err != nil {
		t.Fatal(err)
	}

	if limiter.Active() != 0 {
		t.Fatal("active")
	}
}

func TestPrio(t *testing.T) {
	limiter := New(1)
	var got []int
	limiter.Enter(context.TODO(), 0, "0")
	enterwg := sync.WaitGroup{}
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	enter := func(i int) {
		limiter.Enter(context.TODO(), i, "1")
		lock.Lock()
		got = append(got, i)
		lock.Unlock()
		wg.Done()
	}

	for i := 5; i > 0; i-- {
		wg.Add(1)
		enterwg.Add(1)
		go enter(i)
	}
	todo := 6
	for todo > 0 {
		time.Sleep(time.Millisecond * 50)
		if limiter.Active() > 0 {
			limiter.Leave()
			todo--
		}
	}

	wg.Wait()
	want := []int{1, 2, 3, 4, 5}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestPrioUUID(t *testing.T) {
	limiter := New(1)
	var got []int
	limiter.Enter(context.TODO(), 0, "0")
	enterwg := sync.WaitGroup{}
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	enter := func(i int) {
		limiter.Enter(context.TODO(), 0, fmt.Sprint(i))
		lock.Lock()
		got = append(got, i)
		lock.Unlock()
		wg.Done()
	}

	for i := 5; i > 0; i-- {
		wg.Add(1)
		enterwg.Add(1)
		go enter(i)
	}
	todo := 6
	for todo > 0 {
		time.Sleep(time.Millisecond * 50)
		if limiter.Active() > 0 {
			limiter.Leave()
			todo--
		}
	}

	wg.Wait()
	want := []int{1, 2, 3, 4, 5}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

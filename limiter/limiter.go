package limiter

// ServerLimiter provides interface to limit amount of requests
type ServerLimiter struct {
	limiters map[string]chan struct{}
	limit    int
}

// NewServerLimiter creates a limiter for specific servers list.
func NewServerLimiter(servers []string, l int) ServerLimiter {
	sl := make(map[string]chan struct{})

	for _, s := range servers {
		sl[s] = make(chan struct{}, l)
	}

	return ServerLimiter{
		limiters: sl,
		limit:    l,
	}
}

// Enter claims one of free slots or blocks until there is one.
func (sl ServerLimiter) Enter(s string) {
	if sl.limiters == nil {
		return
	}
	sl.limiters[s] <- struct{}{}
}

// Frees a slot in limiter
func (sl ServerLimiter) Leave(s string) {
	if sl.limiters == nil {
		return
	}
	<-sl.limiters[s]
}

// MaxLimiterUse returns the maximum ratio of limiter saturation in the
// ServerLimiter as a float between 0 and 1.
func (sl ServerLimiter) MaxLimiterUse() float64 {
	max := 0
	for _, limiter := range sl.limiters {
		if l := len(limiter); l > max {
			max = l
		}
	}

	return float64(max) / float64(sl.limit)
}

// LimiterUse returns the ratio of limiter saturation as a float between 0 and
// 1 per limiter.
func (sl ServerLimiter) LimiterUse() map[string]float64 {
	use := make(map[string]float64)
	for name, limiter := range sl.limiters {
		use[name] = float64(len(limiter)) / float64(sl.limit)
	}

	return use
}

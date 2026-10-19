package domain

// transitions is the single source of truth for allowed state changes.
var transitions = map[OrderStatus]map[OrderStatus]bool{
	StatusPendingPayment: {StatusQueued: true, StatusExpired: true},
	StatusQueued:         {StatusInProgress: true, StatusDelayed: true, StatusRefunded: true},
	StatusInProgress:     {StatusCompleted: true, StatusQueued: true, StatusDelayed: true, StatusFailed: true},
	StatusDelayed:        {StatusQueued: true, StatusRefunded: true, StatusFailed: true},
	StatusFailed:         {StatusQueued: true, StatusRefunded: true},
	StatusExpired:        {StatusRefunded: true},
	StatusCompleted:      {},
	StatusRefunded:       {},
}

func CanTransition(from, to OrderStatus) bool { return transitions[from][to] }

func IsTerminal(s OrderStatus) bool {
	t, ok := transitions[s]
	return ok && len(t) == 0
}

func IsKnownStatus(s OrderStatus) bool { _, ok := transitions[s]; return ok }

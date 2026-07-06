package domain

import "testing"

func TestNewOrderStartsPlaced(t *testing.T) {
	o := NewOrder("id1", "SKU-1", 2, 4990)
	if o.Status != StatusPlaced {
		t.Fatalf("status inicial = %q, quero PLACED", o.Status)
	}
	if len(o.History) != 1 || o.History[0] != OrderPlaced {
		t.Fatalf("histórico = %v, quero [OrderPlaced]", o.History)
	}
}

func TestApplyTransitions(t *testing.T) {
	cases := []struct {
		event EventType
		want  Status
	}{
		{PaymentApproved, StatusApproved},
		{PaymentDeclined, StatusDeclined},
		{StockReserved, StatusReserved},
		{StockUnavailable, StatusOutOfStock},
		{OrderShipped, StatusShipped},
	}
	for _, c := range cases {
		o := NewOrder("id", "SKU-1", 1, 100)
		o.Apply(c.event)
		if o.Status != c.want {
			t.Errorf("Apply(%s) = %q, quero %q", c.event, o.Status, c.want)
		}
		last := o.History[len(o.History)-1]
		if last != c.event {
			t.Errorf("histórico não registrou %s (%v)", c.event, o.History)
		}
	}
}

func TestApplyUnknownKeepsStatus(t *testing.T) {
	o := NewOrder("id", "SKU-1", 1, 100)
	o.Apply(OrderPlaced) // evento sem transição definida
	if o.Status != StatusPlaced {
		t.Fatalf("status = %q, quero PLACED", o.Status)
	}
}

func TestAuthorize(t *testing.T) {
	if !Authorize(1000, 1000) {
		t.Error("valor igual ao limite deveria aprovar")
	}
	if Authorize(1001, 1000) {
		t.Error("valor acima do limite deveria recusar")
	}
}

package domain

// Authorize aprova o pagamento quando o valor não excede o limite antifraude.
func Authorize(amountCents, limitCents int) bool {
	return amountCents <= limitCents
}

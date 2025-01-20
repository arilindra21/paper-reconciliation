from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
import itertools
import json
from datetime import timezone

@dataclass
class Payment:
    external_id: str
    amount: float
    date: datetime
    company_id: str
    buyer_name: str

@dataclass
class Invoice:
    invoice_id: str
    amount: float
    date: datetime
    due_date: datetime
    company_id: str
    buyer_name: str
    top: int
    invoice_status: str

@dataclass
class MatchResult:
    payment: Payment | List[Payment]
    invoice: Invoice | List[Invoice]
    status: str
    difference: float
    score: float
    match_type: str = 'single_match'

def calculate_ontime(payment_date, due_date) -> int:
    return 1 if payment_date <= due_date else 0

class PaymentInvoiceMatcher:
    def __init__(self):
        self.TAX_TOLERANCES = [0.0202]  # 2.02%
        self.GENERAL_TOLERANCE = [2000, 5000]
        self.ADD_IDR = 10000  # 10K rule
        self.used_payments: Set[str] = set()
        self.used_invoices: Set[str] = set()
        self.matches = []

    def find_matches(self, payments: List[Payment], invoices: List[Invoice]):
        sorted_payments = sorted(payments, key=lambda x: x.date)
        sorted_invoices = sorted(invoices, key=lambda x: x.date)

        # First try single matches
        self.find_single_matches(sorted_payments, sorted_invoices)
        
        # Then try multi-payment matches
        self.find_multi_payment_matches(sorted_payments, sorted_invoices)
        
        # Finally try multi-invoice matches
        self.find_multi_invoice_matches(sorted_payments, sorted_invoices)

    def find_single_matches(self, payments: List[Payment], invoices: List[Invoice]):
        """Find best 1:1 matches between payments and invoices"""
        for payment in payments:
            if payment.external_id in self.used_payments:
                continue

            best_match = None
            best_score = -float('inf')
            
            for invoice in invoices:
                if invoice.invoice_id in self.used_invoices or payment.date < invoice.date:
                    continue

                    
                match_result = self._evaluate_match(payment, invoice)
                if match_result and match_result.score > best_score:
                    best_match = match_result
                    best_score = match_result.score
            
            if best_match:
                self._add_match(best_match)

    def find_multi_payment_matches(self, payments: List[Payment], invoices: List[Invoice], max_combinations: int = 3):
        """Find matches where multiple payments combine to match one invoice"""
        for invoice in invoices:
            if invoice.invoice_id in self.used_invoices:
                continue

            valid_payments = [p for p in payments 
                            if p.external_id not in self.used_payments 
                            and p.date >= invoice.date]

            best_combo_match = None
            best_combo_score = -float('inf')

            for n in range(2, min(max_combinations + 1, len(valid_payments) + 1)):
                for payment_combo in itertools.combinations(valid_payments, n):
                    total_amount = sum(p.amount for p in payment_combo)
                    
                    combo_match = self._evaluate_multi_payment_match(payment_combo, invoice, total_amount)
                    if combo_match and combo_match.score > best_combo_score:
                        best_combo_match = combo_match
                        best_combo_score = combo_match.score

            if best_combo_match:
                self._add_multi_payment_match(best_combo_match)

    def find_multi_invoice_matches(self, payments: List[Payment], invoices: List[Invoice], max_combinations: int = 3):
        """Find matches where one payment matches multiple invoices"""
        for payment in payments:
            if payment.external_id in self.used_payments:
                continue

            valid_invoices = [i for i in invoices 
                            if i.invoice_id not in self.used_invoices 
                            and payment.date >= i.date]

            best_combo_match = None
            best_combo_score = -float('inf')

            for n in range(2, min(max_combinations + 1, len(valid_invoices) + 1)):
                for invoice_combo in itertools.combinations(valid_invoices, n):
                    total_amount = sum(i.amount for i in invoice_combo)
                    
                    combo_match = self._evaluate_multi_invoice_match(payment, invoice_combo, total_amount)
                    if combo_match and combo_match.score > best_combo_score:
                        best_combo_match = combo_match
                        best_combo_score = combo_match.score

            if best_combo_match:
                self._add_multi_invoice_match(best_combo_match)

    def _evaluate_match(self, payment: Payment, invoice: Invoice) -> Optional[MatchResult]:
        """Evaluate a single payment to single invoice match"""
        """Evaluate a single payment-invoice match and return match details if valid"""
        
        # Exact match (highest priority)
        if payment.amount == invoice.amount:
            return MatchResult(
                payment=payment,
                invoice=invoice,
                status="exactly match",
                difference=0.0,
                score=1000.0
            )

        # Tax matches
        for tax in self.TAX_TOLERANCES:
            amount_with_tax = payment.amount * (1 + tax)
            if amount_with_tax == invoice.amount:
                return MatchResult(
                    payment=payment,
                    invoice=invoice,
                    status=f"exactly match with tax ({tax*100:.2f}%)",
                    difference=abs(payment.amount - invoice.amount),
                    score=900.0 - abs(payment.amount - invoice.amount)
                )

        # 10K Rule
        if invoice.amount == (payment.amount + self.ADD_IDR):
            return MatchResult(
                payment=payment,
                invoice=invoice,
                status="match with add 10K",
                difference=self.ADD_IDR,
                score=800.0 - self.ADD_IDR
            )

        # General Tolerances
        for tolerance in self.GENERAL_TOLERANCE:
            diff = abs(payment.amount - invoice.amount)
            if diff <= tolerance:
                score = 700.0 - diff
                return MatchResult(
                    payment=payment,
                    invoice=invoice,
                    status=f"match with difference: {diff} within tolerance {tolerance}",
                    difference=diff,
                    score=score
                )

            # Check tax combinations
            for tax in self.TAX_TOLERANCES:
                amount_with_tax = payment.amount * (1 + tax)
                diff_with_tax = abs(amount_with_tax - invoice.amount)
                if diff_with_tax <= tolerance:
                    score = 600.0 - diff_with_tax
                    return MatchResult(
                        payment=payment,
                        invoice=invoice,
                        status=f"match with tax ({tax*100:.2f}%) within tolerance {tolerance}",
                        difference=diff_with_tax,
                        score=score
                    )

                # Check tax + 10K combinations
                amount_with_tax_and_10k = amount_with_tax + self.ADD_IDR
                diff_with_tax_and_10k = abs(amount_with_tax_and_10k - invoice.amount)
                if diff_with_tax_and_10k <= tolerance:
                    score = 500.0 - diff_with_tax_and_10k
                    return MatchResult(
                        payment=payment,
                        invoice=invoice,
                        status=f"match with tax ({tax*100:.2f}%) and add 10K within tolerance {tolerance}",
                        difference=diff_with_tax_and_10k,
                        score=score
                    )

            # Check 10K + tolerance
            amount_with_10k = payment.amount + self.ADD_IDR
            diff_with_10k = abs(amount_with_10k - invoice.amount)
            if diff_with_10k <= tolerance:
                score = 600.0 - diff_with_10k
                return MatchResult(
                    payment=payment,
                    invoice=invoice,
                    status=f"match with add 10K within tolerance {tolerance}",
                    difference=diff_with_10k,
                    score=score
                )

        return None

    def _evaluate_multi_payment_match(self, payments: List[Payment], invoice: Invoice, total_amount: float) -> Optional[MatchResult]:
        """Evaluate multiple payments to single invoice match"""
        if total_amount == invoice.amount:
            return MatchResult(
                payment=list(payments),
                invoice=invoice,
                status="multi payment exact match",
                difference=0.0,
                score=950.0,  # Slightly lower than single payment exact match
                match_type='multi_payment'
            )

        # Apply same matching logic as single payments but with slightly lower base scores
        base_score_reduction = 50  # Multi-payment matches score 50 points lower than equivalent single payment matches
        
        # Tax matches
        for tax in self.TAX_TOLERANCES:
            amount_with_tax = total_amount * (1 + tax)
            if amount_with_tax == invoice.amount:
                return MatchResult(
                    payment=list(payments),
                    invoice=invoice,
                    status=f"multi payment match with tax ({tax*100:.2f}%)",
                    difference=abs(total_amount - invoice.amount),
                    score=850.0 - abs(total_amount - invoice.amount),
                    match_type='multi_payment'
                )

        # General Tolerances
        for tolerance in self.GENERAL_TOLERANCE:
            diff = abs(total_amount - invoice.amount)
            if diff <= tolerance:
                score = 650.0 - diff  # Lower base score for multi-payment tolerance matches
                return MatchResult(
                    payment=list(payments),
                    invoice=invoice,
                    status=f"multi payment match with difference: {diff} within tolerance {tolerance}",
                    difference=diff,
                    score=score,
                    match_type='multi_payment'
                )

        return None

    def _evaluate_multi_invoice_match(self, payment: Payment, invoices: List[Invoice], total_amount: float) -> Optional[MatchResult]:
        """Evaluate single payment to multiple invoice match"""
        if payment.amount == total_amount:
            return MatchResult(
                payment=payment,
                invoice=list(invoices),
                status="multi invoice exact match",
                difference=0.0,
                score=950.0,  # Slightly lower than single invoice exact match
                match_type='multi_invoice'
            )

        # Apply same matching logic as single matches but with slightly lower base scores
        base_score_reduction = 50  # Multi-invoice matches score 50 points lower than equivalent single invoice matches
        
        # Tax matches
        for tax in self.TAX_TOLERANCES:
            amount_with_tax = payment.amount * (1 + tax)
            if amount_with_tax == total_amount:
                return MatchResult(
                    payment=payment,
                    invoice=list(invoices),
                    status=f"multi invoice match with tax ({tax*100:.2f}%)",
                    difference=abs(payment.amount - total_amount),
                    score=850.0 - abs(payment.amount - total_amount),
                    match_type='multi_invoice'
                )

        # General Tolerances
        for tolerance in self.GENERAL_TOLERANCE:
            diff = abs(payment.amount - total_amount)
            if diff <= tolerance:
                score = 650.0 - diff  # Lower base score for multi-invoice tolerance matches
                return MatchResult(
                    payment=payment,
                    invoice=list(invoices),
                    status=f"multi invoice match with difference: {diff} within tolerance {tolerance}",
                    difference=diff,
                    score=score,
                    match_type='multi_invoice'
                )

        return None

    def _add_match(self, match_result: MatchResult):
        """Add a single match to the results"""
        """Add a match to the results"""
        ontime = calculate_ontime(match_result.payment.date, match_result.invoice.due_date)
        self.matches.append({
            "company_id": match_result.payment.company_id,
            "buyer_name": match_result.payment.buyer_name,
            "top": match_result.invoice.top,
            "ontime": ontime,
            "payment_date": match_result.payment.date.isoformat(),
            "invoice_date": match_result.invoice.date.isoformat(),
            "invoice_status": match_result.invoice.invoice_status,
            "external_id": match_result.payment.external_id,
            "invoice_number": match_result.invoice.invoice_id,
            "payment_amount": match_result.payment.amount,
            "payment_amount_wht": match_result.payment.amount * (1 + 0.0202),
            "invoice_amount": match_result.invoice.amount,
            "status": match_result.status,
            "difference": match_result.difference,
            "score": match_result.score,
            "type": 'single_match'
        })
        self.used_payments.add(match_result.payment.external_id)
        self.used_invoices.add(match_result.invoice.invoice_id)
        
    def _add_multi_payment_match(self, match_result: MatchResult):
        """Add a multi-payment match to the results"""
        # Get the latest payment date
        latest_payment_date = max(p.date for p in match_result.payment)
        ontime = calculate_ontime(latest_payment_date, match_result.invoice.due_date)
        
        self.matches.append({
            "company_id": match_result.payment[0].company_id,
            "buyer_name": match_result.payment[0].buyer_name,
            "top": match_result.invoice.top,
            "ontime": ontime,
            "payment_date": latest_payment_date.isoformat(),
            "invoice_date": match_result.invoice.date.isoformat(),
            "invoice_status": match_result.invoice.invoice_status,
            "external_id": [p.external_id for p in match_result.payment],
            "invoice_number": match_result.invoice.invoice_id,
            "payment_amount": sum(p.amount for p in match_result.payment),
            "payment_amount_wht": sum(p.amount * (1 + 0.0202) for p in match_result.payment),
            "invoice_amount": match_result.invoice.amount,
            "status": match_result.status,
            "difference": match_result.difference,
            "score": match_result.score,
            "type": 'multi_payment'
        })
        
        # Mark all payments and the invoice as used
        for payment in match_result.payment:
            self.used_payments.add(payment.external_id)
        self.used_invoices.add(match_result.invoice.invoice_id)
        
    def _add_multi_invoice_match(self, match_result: MatchResult):
        """Add a multi-invoice match to the results"""
        # Get the latest due date
        latest_due_date = max(i.due_date for i in match_result.invoice)
        ontime = calculate_ontime(match_result.payment.date, latest_due_date)
        
        self.matches.append({
            "company_id": match_result.payment.company_id,
            "buyer_name": match_result.payment.buyer_name,
            "top": max(i.top for i in match_result.invoice),
            "ontime": ontime,
            "payment_date": match_result.payment.date.isoformat(),
            "invoice_date": [i.date.isoformat() for i in match_result.invoice],
            "invoice_status": match_result.invoice[0].invoice_status,
            "external_id": match_result.payment.external_id,
            "invoice_number": [i.invoice_id for i in match_result.invoice],
            "payment_amount": match_result.payment.amount,
            "payment_amount_wht": match_result.payment.amount * (1 + 0.0202),
            "invoice_amount": sum(i.amount for i in match_result.invoice),
            "status": match_result.status,
            "difference": match_result.difference,
            "score": match_result.score,
            "type": 'multi_invoice'
        })
        
        # Mark payment and all invoices as used
        self.used_payments.add(match_result.payment.external_id)
        for invoice in match_result.invoice:
            self.used_invoices.add(invoice.invoice_id)

def match_payments_and_invoices(raw_payments: List[Dict], raw_invoices: List[Dict]) -> Dict:
    """Main function to process raw data and return matches"""
    # Parse the data
    payments = [parse_payment(p) for p in raw_payments]
    invoices = [parse_invoice(i) for i in raw_invoices]
    
    # Create matcher and find matches
    matcher = PaymentInvoiceMatcher()
    matcher.find_matches(payments, invoices)
    
    # Get unmatched items
    unmatched_payments = [
        {"external_id": p.external_id, "amount": p.amount}
        for p in payments 
        if p.external_id not in matcher.used_payments
    ]
    
    unmatched_invoices = [
        {"invoice_id": i.invoice_id, "amount": i.amount}
        for i in invoices 
        if i.invoice_id not in matcher.used_invoices
    ]
    
    return {
        "matches": matcher.matches,
        "unmatched_payments": unmatched_payments,
        "unmatched_invoices": unmatched_invoices
    }

def parse_payment(raw_payment: Dict) -> Payment:
    """Convert raw payment data to Payment object"""
    date_str = raw_payment['created_at'].replace('Z', '+00:00')
    try:
        date = datetime.fromisoformat(date_str)
        date = date.astimezone(timezone.utc)
        date = date.replace(tzinfo=None)
    except ValueError:
        date = datetime.fromisoformat(date_str.split('+')[0])

    return Payment(
        external_id=raw_payment['external_id'],
        amount=float(raw_payment['amount.grand_total']),
        date=date,
        company_id=raw_payment['company_id'],
        buyer_name=raw_payment['buyer_name']
    )

def parse_invoice(raw_invoice: Dict) -> Invoice:
    """Convert raw invoice data to Invoice object"""
    
    date = datetime.strptime(raw_invoice['invoice_date'], '%Y-%m-%d')
    due_date = datetime.strptime(raw_invoice['due_date'], '%Y-%m-%d')
    return Invoice(
        invoice_id=raw_invoice['invoice_number'],
        amount=float(raw_invoice['grandTotalUnformatted']),
        date=date,
        due_date=due_date,
        company_id=raw_invoice['company_id'],
        buyer_name=raw_invoice['name'],
        top=int(raw_invoice['top']),
        invoice_status=raw_invoice['invoice_status']
    )
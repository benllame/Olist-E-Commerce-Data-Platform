"""
test_beam_pipelines.py

Tests unitarios para pipelines Apache Beam
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from datetime import datetime

# ==========================================
# TESTS PARA PARSERS
# ==========================================

class TestBeamParsers(unittest.TestCase):
    """Tests para DoFns de parsing"""
    
    def test_parse_order_csv_valid(self):
        """Test parsing de CSV válido"""
        
        # Simular ParseOrder DoFn
        class ParseOrder(beam.DoFn):
            def process(self, element):
                order = {
                    'order_id': element['order_id'],
                    'customer_id': element['customer_id'],
                    'order_status': element['order_status'],
                    'year': element['order_purchase_timestamp'].year,
                    'month': element['order_purchase_timestamp'].month,
                }
                yield order
        
        input_data = [{
            'order_id': 'abc123',
            'customer_id': 'cust456',
            'order_status': 'delivered',
            'order_purchase_timestamp': datetime(2024, 1, 15)
        }]
        
        expected = [{
            'order_id': 'abc123',
            'customer_id': 'cust456',
            'order_status': 'delivered',
            'year': 2024,
            'month': 1,
        }]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(ParseOrder())
            )
            
            assert_that(output, equal_to(expected))
    
    def test_parse_order_csv_missing_field(self):
        """Test parsing con campo faltante"""
        
        class ParseOrder(beam.DoFn):
            def process(self, element):
                try:
                    order = {
                        'order_id': element['order_id'],
                        'customer_id': element['customer_id'],
                    }
                    yield order
                except KeyError as e:
                    # Log error pero no yield (descarta elemento)
                    pass
        
        input_data = [
            {'order_id': 'abc123'},  # Missing customer_id
        ]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(input_data)
                | beam.ParDo(ParseOrder())
            )
            
            # No debería haber output
            assert_that(output, equal_to([]))

# ==========================================
# TESTS PARA TRANSFORMACIONES
# ==========================================

class TestBeamTransformations(unittest.TestCase):
    """Tests para transformaciones complejas"""
    
    def test_enrichment_with_payment(self):
        """Test enriquecimiento con info de pago"""
        
        class EnrichWithPayment(beam.DoFn):
            def process(self, element, payments_dict):
                order_id = element['order_id']
                if order_id in payments_dict:
                    payment = payments_dict[order_id]
                    element['payment_type'] = payment.get('payment_type')
                    element['payment_value'] = payment.get('payment_value')
                yield element
        
        orders = [{'order_id': 'ord123', 'customer_id': 'cust456'}]
        payments = {
            'ord123': {'payment_type': 'credit_card', 'payment_value': 150.00}
        }
        
        with TestPipeline() as p:
            payments_pcoll = p | 'Create Payments' >> beam.Create([('ord123', payments['ord123'])])
            payments_dict = beam.pvalue.AsDict(payments_pcoll)
            
            output = (
                p
                | 'Create Orders' >> beam.Create(orders)
                | 'Enrich' >> beam.ParDo(EnrichWithPayment(), payments_dict=payments_dict)
            )
            
            expected = [{
                'order_id': 'ord123',
                'customer_id': 'cust456',
                'payment_type': 'credit_card',
                'payment_value': 150.00
            }]
            
            assert_that(output, equal_to(expected))
    
    def test_aggregation_by_month(self):
        """Test agregación por mes"""
        
        orders = [
            {'order_id': '1', 'year_month': '2024-01', 'payment_value': 100},
            {'order_id': '2', 'year_month': '2024-01', 'payment_value': 200},
            {'order_id': '3', 'year_month': '2024-02', 'payment_value': 150},
        ]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(orders)
                | beam.Map(lambda x: (x['year_month'], x['payment_value']))
                | beam.CombinePerKey(sum)
            )
            
            expected = [
                ('2024-01', 300),
                ('2024-02', 150),
            ]
            
            assert_that(output, equal_to(expected))

# ==========================================
# TESTS PARA VALIDACIONES
# ==========================================

class TestDataValidation(unittest.TestCase):
    """Tests para validaciones de datos"""
    
    def test_order_id_format_valid(self):
        """Test formato de order_id válido"""
        
        def validate_order_id(order_id):
            return len(order_id) == 32 and order_id.isalnum()
        
        valid_id = 'abc123def456ghi789jkl012mno345pq'
        self.assertTrue(validate_order_id(valid_id))
    
    def test_order_id_format_invalid(self):
        """Test formato de order_id inválido"""
        
        def validate_order_id(order_id):
            return len(order_id) == 32 and order_id.isalnum()
        
        invalid_ids = [
            'short',                              # Muy corto
            'abc123-def456-ghi789-jkl012-mno345',  # Con guiones
            'abc123def456ghi789jkl012mno345pqrst', # Muy largo
        ]
        
        for invalid_id in invalid_ids:
            self.assertFalse(validate_order_id(invalid_id))
    
    def test_payment_value_valid(self):
        """Test payment_value en rango válido"""
        
        def validate_payment_value(value):
            return 0 < value < 10000
        
        self.assertTrue(validate_payment_value(100.50))
        self.assertTrue(validate_payment_value(9999.99))
    
    def test_payment_value_invalid(self):
        """Test payment_value fuera de rango"""
        
        def validate_payment_value(value):
            return 0 < value < 10000
        
        self.assertFalse(validate_payment_value(-10))
        self.assertFalse(validate_payment_value(0))
        self.assertFalse(validate_payment_value(15000))

# ==========================================
# RUN TESTS
# ==========================================

if __name__ == '__main__':
    unittest.main()
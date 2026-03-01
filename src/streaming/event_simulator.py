"""
event_simulator.py

Simula eventos de órdenes de Olist y los publica a Pub/Sub
"""

from google.cloud import pubsub_v1
import json
import random
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

# ==========================================
# CONFIGURACIÓN
# ==========================================

PROJECT_ID = 'ecommerce-olist-150226'  # ⚠️ Cambiar
TOPIC_NAME = 'olist-order-events'

# Datos de ejemplo
CUSTOMER_IDS = [f'customer_{i:06d}' for i in range(1, 10001)]
PRODUCT_IDS = [f'product_{i:05d}' for i in range(1, 1001)]
ORDER_STATUSES = ['created', 'approved', 'processing', 'shipped', 'delivered', 'canceled']
EVENT_TYPES = ['order_created', 'order_updated', 'payment_received', 'order_shipped', 'order_delivered']

# ==========================================
# SIMULADOR
# ==========================================

class OrderEventSimulator:
    """Genera y publica eventos de órdenes simulados"""
    
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_path = f'projects/{project_id}/topics/{topic_name}'
        
        # Crear publisher
        self.publisher = pubsub_v1.PublisherClient()
        
        logging.info(f"Simulator initialized")
        logging.info(f"Publishing to: {self.topic_path}")
    
    def generate_order_event(self):
        """Genera un evento de orden aleatorio"""
        
        event_type = random.choice(EVENT_TYPES)
        
        event = {
            'event_id': f'event_{int(time.time() * 1000)}_{random.randint(1000, 9999)}',
            'event_type': event_type,
            'order_id': f'order_{random.randint(100000, 999999)}',
            'customer_id': random.choice(CUSTOMER_IDS),
            'product_id': random.choice(PRODUCT_IDS),
            'order_status': random.choice(ORDER_STATUSES),
            'payment_value': round(random.uniform(10.0, 500.0), 2),
            'event_timestamp': datetime.utcnow().isoformat() + 'Z',
            'metadata': {
                'source': 'simulator',
                'version': '1.0'
            }
        }
        
        return event
    
    def publish_event(self, event):
        """Publica evento a Pub/Sub"""
        
        # Convertir a JSON y encodear
        message_data = json.dumps(event).encode('utf-8')
        
        # Publicar
        future = self.publisher.publish(self.topic_path, message_data)
        
        try:
            message_id = future.result(timeout=5.0)
            logging.info(f"Published: {event['event_type']} | Order: {event['order_id']} | ID: {message_id}")
            return message_id
        except Exception as e:
            logging.error(f"Error publishing: {e}")
            return None
    
    def run(self, events_per_second=1, duration_seconds=None):
        """
        Ejecuta simulador
        
        Args:
            events_per_second: Número de eventos por segundo
            duration_seconds: Duración en segundos (None = infinito)
        """
        
        logging.info("=" * 80)
        logging.info("ORDER EVENT SIMULATOR STARTED")
        logging.info("=" * 80)
        logging.info(f"Events per second: {events_per_second}")
        logging.info(f"Duration: {'infinite' if duration_seconds is None else f'{duration_seconds}s'}")
        logging.info("Press Ctrl+C to stop")
        logging.info("=" * 80)
        
        event_count = 0
        start_time = time.time()
        
        try:
            while True:
                # Generar y publicar evento
                event = self.generate_order_event()
                self.publish_event(event)
                
                event_count += 1
                
                # Estadísticas cada 10 eventos
                if event_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    logging.info(f"Stats: {event_count} events | {rate:.2f} events/s")
                
                # Verificar duración
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    logging.info(f"Duration reached: {duration_seconds}s")
                    break
                
                # Esperar según rate
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            logging.info("\n Simulator stopped by user")
        
        finally:
            elapsed = time.time() - start_time
            logging.info("=" * 80)
            logging.info("FINAL STATS")
            logging.info("=" * 80)
            logging.info(f"Total events published: {event_count}")
            logging.info(f"Total duration: {elapsed:.2f}s")
            logging.info(f"Average rate: {event_count / elapsed:.2f} events/s")
            logging.info("=" * 80)

# ==========================================
# MAIN
# ==========================================

def main():
    """Función principal"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Olist Order Event Simulator')
    parser.add_argument(
        '--project',
        type=str,
        default=PROJECT_ID,
        help='GCP Project ID'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default=TOPIC_NAME,
        help='Pub/Sub topic name'
    )
    parser.add_argument(
        '--rate',
        type=float,
        default=1.0,
        help='Events per second (default: 1.0)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Crear simulador
    simulator = OrderEventSimulator(args.project, args.topic)
    
    # Ejecutar
    simulator.run(
        events_per_second=args.rate,
        duration_seconds=args.duration
    )

if __name__ == '__main__':
    main()

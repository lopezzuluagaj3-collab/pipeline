# Modelado de Datos

El modelo usa una tabla de hechos `fact_ventas` y dimensiones de fecha, producto, cliente y pais-region.

## Grano

Cada fila de `fact_ventas` representa una linea de orden unica por `ordernumber` + `orderlinenumber`.

## Ventajas

- analisis temporal y geografico simple,
- separacion clara entre catalogos y transacciones,
- control de duplicados con `ON CONFLICT DO NOTHING`.

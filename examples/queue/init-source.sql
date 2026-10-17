-- A table with an integer primary key, so the planner can cut it into key ranges. Two million rows.
CREATE TABLE public.orders (
    id          bigint PRIMARY KEY,
    customer    text        NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    created_at  timestamptz NOT NULL
);

INSERT INTO public.orders
SELECT i,
       'customer-' || (i % 5000),
       round((random() * 1000)::numeric, 2),
       now() - (i || ' seconds')::interval
FROM generate_series(1, 2000000) AS i;

ANALYZE public.orders;

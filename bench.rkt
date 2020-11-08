#lang racket/base

(module fast-csv racket/base
  (require "fast-csv.rkt")
  (define reader (make-fast-csv-reader (current-input-port) 4096))
  (for ([rec (in-producer reader #f)]) (void)))

(module csv-reading racket/base
  (require csv-reading)
  (define reader (make-csv-reader (current-input-port)))
  (for ([rec (in-producer reader null?)]) (void)))

#lang racket/base

(require racket/class
         racket/dict
         racket/pretty

         disposable
         "fast-csv.rkt"
         "fix-columns.rkt"
         "pagefile.rkt")

(define csv-read (make-fast-csv-reader (current-input-port)))
(define fields (map (compose1 string->symbol bytes->string/utf-8) (csv-read)))

(define (read1)
  (define vals (csv-read))
  (if vals
      (map cons fields vals)
      null))

(define ZERO (char->integer #\0))

(define (s->i s)
  (for/fold ([n 0]) ([b (in-bytes s)])
    (+ (* n 10) (- b ZERO))))

(define (pagefile/d filename #:pagesize pagesize)
  (disposable (lambda () (open-pagefile filename #:pagesize pagesize))
              close-pagefile))

(define (fixed-column/d filename #:pagesize pagesize #:record-size rec-size)
  (disposable-chain (pagefile/d filename #:pagesize pagesize)
                    (lambda (pf)
                      (disposable (lambda () (fixed-column pf rec-size))
                                  void))))

(define (fc-writer/d filename #:pagesize pagesize #:record-size rec-size)
  (disposable-chain (fixed-column/d filename #:pagesize pagesize #:record-size rec-size)
                    (lambda (fc)
                      (make-disposable
                        (lambda ()
                          (define wr
                            (new fixed-column-writer% [fc fc]))
                          (values wr (lambda () (send wr flush-page!))))))))

(with-disposable ([idf (fc-writer/d "id.cdb" #:pagesize 1024 #:record-size 8)])
  (for ([r (in-producer read1 null?)])
    (define id (s->i (dict-ref r 'id)))
    (define vbuf (integer->integer-bytes id 8 #f #f))
    (send idf write-record! vbuf)))


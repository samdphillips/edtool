#lang racket/base

(require "fast-csv.rkt")

(define csv-reader (make-fast-csv-reader (current-input-port)))
(define fields (csv-reader))

(define (make-getter xs v)
  (cond
    [(null? xs) (error 'make-getter "cannot find element ~v" v)]
    [(equal? (car xs) v) car]
    [else
      (compose1 (make-getter (cdr xs) v) cdr)]))

(define get-x (make-getter fields #"x"))
(define get-y (make-getter fields #"y"))
(define get-z (make-getter fields #"z"))

(define (byte-index bs v)
  (for/first ([i (in-naturals)] [b (in-bytes bs)] #:when (= b v))  i))

(define DASH (char->integer #\-))
(define DOT (char->integer #\.))

(define (split-val bs)
  (define len (bytes-length bs))
  (define neg? (= (bytes-ref bs 0) DASH))
  (define i (byte-index bs DOT))
  (cond
    [(not i) (values (if neg? (sub1 len) len) 0)]
    [else
      (values (if neg? (sub1 i) i)
              (- (bytes-length bs) 1 i))]))

(for/fold ([a 0] [b 0]) ([rec (in-producer csv-reader #f)])
  (define-values (xa xb) (split-val (get-x rec)))
  (define-values (ya yb) (split-val (get-y rec)))
  (define-values (za zb) (split-val (get-z rec)))
  (values (max xa ya za) (max xb yb zb)))

#|

(define (read1)
  (define vals (csv-reader))
  (if (null? vals)
      vals
      (map cons fields vals)))

(define ZERO (char->integer #\0))

(define (s->i s)
  (for/fold ([n 0]) ([c (in-string s)])
    (+ (* n 10) (- (char->integer c) ZERO))))

(call-with-output-file "id.cdb"
  #:exists 'replace
  (lambda (outp)
    (define wr (new write-cursor% [outp outp]))

    (for ([r (in-producer read1 null?)])
      (define id (s->i (dict-ref r 'id)))
      (define vbuf (integer->integer-bytes id 8 #f #f))
      (send wr write-record! vbuf))
    (send wr flush-page!)))
|#

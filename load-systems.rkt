#lang racket/base

(require racket/class
         racket/dict
         racket/pretty
         csv-reading
         "columns.rkt")

(define csv-reader (make-csv-reader (current-input-port)))
(define fields (map string->symbol (csv-reader)))

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


#lang racket/base

(require racket/class
         racket/contract)

(define tag #"RCOL")
(define version (bytes 0 1))
(define header-size 16)

(define record-size      8)
(define page-size        4096)
(define page-header-size 16)

(define records-per-page
  (quotient (- page-size page-header-size) record-size))

(define (page-offset pi)
  (+ header-size (* pi page-size)))

(define (record-offset ri)
  (+ page-header-size (* ri record-size)))

(define (page-file-port? p)
  (or (file-stream-port? p)
      (string-port? p)))

(define (buffer/c size)
  (and/c bytes? (property/c bytes-length size)))

(define input-page-file/c (and/c input-port? page-file-port?))
(define output-page-file/c (and/c output-port? page-file-port?))

(define/contract (pagefile-header inp)
  (-> input-page-file/c (buffer/c header-size))
  (file-position inp 0)
  (read-bytes header-size inp))

(define/contract (pagefile-header-set! outp buf)
  (-> output-page-file/c (buffer/c header-size) any)
  (file-position outp 0)
  (write-bytes buf outp))

(define/contract (page-ref inp i)
  (-> input-page-file/c exact-nonnegative-integer? (buffer/c page-size))
  (file-position inp (page-offset i))
  (read-bytes page-size inp))

(define/contract (page-set! outp i buf)
  (-> output-page-file/c exact-nonnegative-integer? (buffer/c page-size) any)
  (file-position outp (page-offset i))
  (write-bytes buf outp))

(define (header-num-pages-set! hdr i)
  (-> (buffer/c header-size) exact-nonnegative-integer? any)
  (integer->integer-bytes i 4 #f #f hdr 6))

(define/contract (initialize-file! outp)
  (-> output-page-file/c (buffer/c header-size))
  (define hdr (make-bytes header-size))
  (bytes-copy! hdr 0 tag)
  (bytes-copy! hdr 4 version)
  (header-num-pages-set! hdr 0)
  (file-position outp 0)
  (write-bytes hdr outp)
  hdr)

(define write-cursor%
  (class object%
    (init-field outp)

    (define (make-page)
      (define buf (make-bytes page-size))
      (bytes-copy! buf 0 (bytes #xbe #xef))
      buf)

    (field [page-index 0]
           [page-buf (make-page)]
           [ridx 0])
    (field [header (initialize-file! outp)])
    (super-new)

    (define (page-full?)
      (= ridx records-per-page))

    (define/public (flush-page!)
      (integer->integer-bytes ridx 4 #f #f page-buf 2)
      (page-set! outp page-index page-buf)
      (set! page-index (add1 page-index))
      (header-num-pages-set! header page-index)
      (pagefile-header-set! outp header)
      (set! ridx 0)
      (set! page-buf (make-page)))

    (define/public (write-record! rec-buf)
      (bytes-copy! page-buf (record-offset ridx) rec-buf)
      (set! ridx (add1 ridx))
      (when (page-full?) (flush-page!)))))

(provide write-cursor%)

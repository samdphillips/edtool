#lang racket/base

(require racket/class
         "pagefile.rkt")

(provide (struct-out fixed-column)
         fixed-column-writer%)

(define-logger fixed-column)

(define (peek-int buf start size signed?)
  (integer-bytes->integer buf signed? #t start (+ start size)))

(define (poke-int buf v start size signed?)
  (integer->integer-bytes v size signed? #t buf start))

(struct fixed-column [pf record-size])

(define HEADER-TAG #"CFIX")
(define VERSION 1)

(define (header-tag-ref p)  (subbytes p 0 4))
(define (header-tag-set! p) (bytes-copy! p 0 HEADER-TAG))

(define (header-version-ref p)  (peek-int p 4 4 #f))
(define (header-version-set! p) (poke-int p VERSION 4 4 #f))

(define (header-page-count-ref p)    (peek-int p 8 4 #f))
(define (header-page-count-set! p c) (poke-int p c 8 4 #f))

(define (header-record-size-ref p)    (peek-int p 12 1 #f))
(define (header-record-size-set! p z) (poke-int p z 12 1 #f))

(define RECORDS-TAG #"FREC")
(define RECORD-HEADER-SIZE 8)

(define (records-tag-ref p)  (subbytes p 0 4))
(define (records-tag-set! p) (bytes-copy! p 0 RECORDS-TAG))

(define (records-count-ref p)    (peek-int p 4 4 #f))
(define (records-count-set! p c) (poke-int p c 4 4 #f))

(define (fixed-column-records-per-page fc)
  (quotient (- (pagefile-pagesize (fixed-column-pf fc))
               RECORD-HEADER-SIZE)
            (fixed-column-record-size fc)))

(define (empty-page fc)
  (make-bytes (pagefile-pagesize (fixed-column-pf fc))))

(define (make-record-page fc)
  (define p (empty-page fc))
  (records-tag-set! p)
  (records-count-set! p 0)
  p)

(define (initialize-file! fc)
  (define hdr (empty-page fc))
  (header-tag-set! hdr)
  (header-version-set! hdr)
  (header-page-count-set! hdr 0)
  (page-set! (fixed-column-pf fc) 0 hdr)
  hdr)

(define fixed-column-writer%
  (class object%
    (init-field fc)

    (field [page-index 0]
           [page-buf (make-record-page fc)]
           [ridx 0])
    (field [header (initialize-file! fc)])
    (super-new)

    (define record-size (fixed-column-record-size fc))

    (define records-per-page
      (fixed-column-records-per-page fc))

    (define pf (fixed-column-pf fc))

    (define (page-full?)
      (= ridx records-per-page))

    (define (record-offset ri)
      (+ RECORD-HEADER-SIZE (* ri record-size)))

    (define/public (flush-page!)
      (records-count-set! page-buf ridx)
      (page-set! pf page-index page-buf)
      (set! page-index (add1 page-index))
      (header-page-count-set! header page-index)
      (page-set! pf 0 header)
      (set! ridx 0)
      (set! page-buf (make-record-page fc)))

    (define/public (write-record! rec-buf)
      (log-fixed-column-debug "ridx: ~a rpp: ~a" ridx records-per-page)
      (bytes-copy! page-buf (record-offset ridx) rec-buf)
      (set! ridx (add1 ridx))
      (when (page-full?) (flush-page!)))))

#|
(define version (bytes 0 1))
(define header-size 16)

(define record-size      8)
(define page-size        4096)
(define page-header-size 16)

(define records-per-page
  (quotient (- page-size page-header-size) record-size))

(define (page-offset pi)
  (+ header-size (* pi page-size)))

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

(provide write-cursor%)
|#

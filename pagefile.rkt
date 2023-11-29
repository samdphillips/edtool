#lang racket/base

(require racket/contract
         racket/match
         racket/math)

(define (buffer/c size)
  (and/c bytes? (property/c bytes-length size)))

(provide
  (contract-out
    #:unprotected-submodule no-contract
    [pagefile? (-> any/c boolean?)]
    [pagefile-pagesize (-> pagefile? nonnegative-integer?)]
    [open-pagefile
      (->* (path-string?)
           (#:exists (or/c 'error 'append 'update 'can-update
                           'replace 'truncate 'truncate/replace)
            #:pagesize positive-integer?)
           pagefile?)]
    [close-pagefile (-> pagefile? any)]
    [page-ref
      (->i ([pf pagefile?]
            [i  nonnegative-integer?])
           (_ (pf) (buffer/c (pagefile-pagesize pf))))]
    [page-set!
      (->i ([pf pagefile?]
            [i  nonnegative-integer?]
            [buf (pf) (buffer/c (pagefile-pagesize pf))])
           any)]))

(struct pagefile [in-port out-port pagesize])

(define (open-pagefile filename
                       #:exists [exists 'error]
                       #:pagesize [pagesize 1024])
  (define-values (in-port out-port)
    (open-input-output-file filename #:exists exists))
  (pagefile in-port out-port pagesize))

(define (close-pagefile pf)
  (match-define (pagefile inp outp _) pf)
  (close-input-port inp)
  (close-output-port outp))

(define (page-offset page-size i) (* i page-size))

(define (page-ref pf i)
  (match-define (pagefile inp _ psize) pf)
  (file-position inp (page-offset psize i))
  (read-bytes psize inp))

(define (page-set! pf i buf)
  (match-define (pagefile _ outp psize) pf)
  (file-position outp (page-offset psize i))
  (void (write-bytes buf outp)))


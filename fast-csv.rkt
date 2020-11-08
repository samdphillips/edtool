#lang racket/base

(require racket/match)

(provide make-fast-csv-reader)

(module validate racket/base
  (require racket/contract
           racket/math)

  (provide (all-from-out racket/contract)
           statef/c)

  (define (different-heads? xs ys)
    (cond
      [(null? xs) #t]
      [(null? ys) #t]
      [else
        (not (= (car xs) (car ys)))]))
  
  (define (field-token? v)
    (and (pair? v)
         (let ([a (car v)]
               [d (cdr v)])
           (<= a d))))
  
  (define statef/c
    (->i ([i nonnegative-integer?]
          [cs (i) (or/c null? (cons/c (>=/c i) (listof nonnegative-integer?)))]
          [qs (i) (or/c null? (cons/c (>=/c i) (listof nonnegative-integer?)))]
          [bs (i) (or/c null? (cons/c (>=/c i) (listof nonnegative-integer?)))])
         #:pre (cs qs bs)
         (and (different-heads? cs qs)
              (different-heads? qs bs)
              (different-heads? cs bs))
         [_ (listof (or/c 'break field-token?))])))

;(require 'validate)
(define-syntax-rule (define/contract x ctc body ...) (define x body ...))

(define (last-break buf)
  (for/or ([i (in-range (sub1 (bytes-length buf)) -1 -1)])
    (if (= (bytes-ref buf i) #x0a) i #f)))

(define (scan-indexes buf [blen (bytes-length buf)])
  (for/fold ([commas null]
             [quotes null]
             [breaks null])
            ([i (in-range (sub1 blen) -1 -1)])
    (define b (bytes-ref buf i))
    (values (if (= b #x2c) (cons i commas) commas)
            (if (= b #x22) (cons i quotes) quotes)
            (if (= b #x0a) (cons i breaks) breaks))))

(define (minq cs qs bs)
  (define-syntax-rule (car/f v)
    (if (null? v) #f (car v)))
  (define-syntax-rule (min2 a ta b tb)
    (cond
      [(not a) (values b tb)]
      [(not b) (values a ta)]
      [(< a b) (values a ta)]
      [else    (values b tb)]))
  (cond
    [(null? bs) null]
    [else
      (define c (car/f cs))
      (define q (car/f qs))
      (define b (car/f bs))
      (define-values (v tv) (min2 c 'comma q 'quote))
      (define-values (w tw) (min2 v tv b 'break))
      tw]))

(define (indexes->tokens cs qs bs)
  (define/contract (in-field i cs qs bs) statef/c
    (match (minq cs qs bs)
      ['comma (cons (cons i (car cs))
                    (next-field (car cs) (cdr cs) qs bs))]
      ['break (list* (cons i (car bs))
                     'break
                     (next-field (car bs) cs qs (cdr bs)))]
      ['() null]))

  (define/contract (next-field i cs qs bs) statef/c
    (match (minq cs qs bs)
      [(or 'comma 'break) (in-field (add1 i) cs qs bs)]
      ['quote             (in-quote (add1 (car qs)) cs (cdr qs) bs)]
      ['()                null]))

  (define/contract (in-quote i cs qs bs) statef/c
    (match (minq cs qs bs)
      ['comma (in-quote i (cdr cs) qs bs)]
      ['break (in-quote i cs qs (cdr bs))]
      ['quote (cons (cons i (car qs))
                    (end-quote (car qs) cs (cdr qs) bs))]))

  (define/contract (end-quote i cs qs bs) statef/c
    (match (minq cs qs bs)
      ['comma (next-field (car cs) (cdr cs) qs bs)]
      ['break (cons 'break (next-field (car bs) cs qs (cdr bs)))]))

  (in-field 0 cs qs bs))

(define (make-fast-csv-reader inp [bufsize 1024])
  (define buf (make-bytes bufsize))
  (define insert-pos 0)
  (define at-end? #f)
  (define (build-records tokens)
    (match tokens
      ['() null]
      [(cons 'break rest) (build-records rest)]
      [(list* (and (not 'break) fs) ... rest)
       (cons (for/list ([tok (in-list fs)])
               (subbytes buf (car tok) (cdr tok)))
             (build-records rest))]))
  (define (read-chunk)
    (define read-len (read-bytes! buf inp insert-pos))
    (set! at-end? (eof-object? (peek-byte inp)))
    (define scan-end
      (if at-end?
          (+ insert-pos read-len)
          (last-break buf)))
    (define-values (cs qs bs) (scan-indexes buf scan-end))
    (define tokens (indexes->tokens cs qs bs))
    (define records (build-records tokens))
    (bytes-copy! buf 0 buf (add1 scan-end))
    (set! insert-pos (- bufsize scan-end 1))
    records)
  (lambda ()
    (and (not at-end?)
         (read-chunk))))


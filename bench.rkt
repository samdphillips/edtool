#lang racket/base

(module fast-csv racket/base
  (require "fast-csv.rkt")
  (define run-custodian (make-custodian))
  (custodian-limit-memory run-custodian (* 10 1024 1024))

  (define run-thread
    (parameterize ([current-custodian run-custodian])
      (define inp (current-input-port))
      (thread
        (lambda ()
          (define reader (make-fast-csv-reader inp 4096))
          (displayln
            (time (for/fold ([x 0]) ([recs (in-producer reader #f)]) (add1 x))))))))
  (sync
    (handle-evt
      (alarm-evt (+ 60000 (current-inexact-milliseconds)))
      (lambda (e) (error 'timeout)))
    (handle-evt run-thread
      (lambda (th)
        (when (custodian-shut-down? run-custodian)
          (error 'oom))))))

(module csv-reading racket/base
  (require csv-reading)
  (define reader (make-csv-reader (current-input-port)))
  (time (for/fold ([x 0]) ([rec (in-producer reader null?)]) (add1 x))))

(module fast-csv-check racket/base
  (require "fast-csv.rkt")
  (define reader (make-fast-csv-reader (current-input-port) 4096))
  (for ([recs (in-producer reader #f)])
    (for ([rec (in-list recs)])
      (writeln (map bytes->string/utf-8 rec)))))

(module csv-reading-check racket/base
  (require csv-reading)
  (define reader (make-csv-reader (current-input-port)))
  (for ([rec (in-producer reader null?)])
    (writeln rec)))


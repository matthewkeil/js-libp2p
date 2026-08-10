/**
 * @packageDocumentation
 *
 * A [libp2p transport](https://libp2p.io/docs/transports-overview/) based on the TCP networking stack.
 *
 * @example
 *
 * ```TypeScript
 * import { createLibp2p } from 'libp2p'
 * import { tcp } from '@libp2p/tcp'
 * import { multiaddr } from '@multiformats/multiaddr'
 *
 * const node = await createLibp2p({
 *   transports: [
 *     tcp()
 *   ]
 * })
 *
 * const ma = multiaddr('/ip4/123.123.123.123/tcp/1234')
 *
 * // dial a TCP connection, timing out after 10 seconds
 * const connection = await node.dial(ma, {
 *   signal: AbortSignal.timeout(10_000)
 * })
 *
 * // use connection...
 * ```
 */

import net from 'net'
import { AbortError, NotStartedError, TimeoutError, serviceCapabilities, transportSymbol } from '@libp2p/interface'
import { TCP as TCPMatcher } from '@multiformats/multiaddr-matcher'
import { anySignal } from 'any-signal'
import { setMaxListeners } from 'main-event'
import { CustomProgressEvent } from 'progress-events'
import { TCPListener } from './listener.ts'
import { toMultiaddrConnection } from './socket-to-conn.ts'
import { multiaddrToNetConfig } from './utils.ts'
import type { TCPComponents, TCPCreateListenerOptions, TCPDialEvents, TCPDialOptions, TCPMetrics, TCPOptions } from './index.ts'
import type { Logger, Connection, Transport, Listener, MultiaddrConnection } from '@libp2p/interface'
import type { Multiaddr } from '@multiformats/multiaddr'
import type { Socket, IpcSocketConnectOpts, TcpSocketConnectOpts } from 'net'

async function closeSocket (socket: Socket): Promise<void> {
  if (socket.closed) {
    return
  }

  const closed = new Promise<void>((resolve) => {
    socket.once('close', () => {
      resolve()
    })
  })

  socket.destroy()
  await closed
}

export class TCP implements Transport<TCPDialEvents> {
  private readonly opts: TCPOptions
  private readonly metrics?: TCPMetrics
  private readonly components: TCPComponents
  private readonly log: Logger
  private readonly pendingDialOperations: Set<Promise<void>>
  private acceptingDials: boolean
  private shutdownController: AbortController
  private shutdownPromise?: Promise<void>

  constructor (components: TCPComponents, options: TCPOptions = {}) {
    this.log = components.logger.forComponent('libp2p:tcp')
    this.opts = options
    this.components = components
    this.pendingDialOperations = new Set()
    this.acceptingDials = true
    this.shutdownController = new AbortController()
    setMaxListeners(Infinity, this.shutdownController.signal)

    if (components.metrics != null) {
      this.metrics = {
        events: components.metrics.registerCounterGroup('libp2p_tcp_dialer_events_total', {
          label: 'event',
          help: 'Total count of TCP dialer events by type'
        }),
        errors: components.metrics.registerCounterGroup('libp2p_tcp_dialer_errors_total', {
          label: 'event',
          help: 'Total count of TCP dialer events by type'
        })
      }
    }
  }

  readonly [transportSymbol] = true

  readonly [Symbol.toStringTag] = '@libp2p/tcp'

  readonly [serviceCapabilities]: string[] = [
    '@libp2p/transport'
  ]

  start (): void {
    this.acceptingDials = true
    this.shutdownController = new AbortController()
    setMaxListeners(Infinity, this.shutdownController.signal)
    this.shutdownPromise = undefined
  }

  async beforeStop (): Promise<void> {
    if (this.shutdownPromise != null) {
      return this.shutdownPromise
    }

    this.acceptingDials = false
    this.shutdownController.abort(new AbortError('TCP transport is stopping'))

    // Every admitted dial registers its completion before invoking user code,
    // so this set cannot grow after acceptingDials is cleared.
    this.shutdownPromise = Promise.all([...this.pendingDialOperations]).then(() => undefined)
    await this.shutdownPromise
  }

  async stop (): Promise<void> {
    await this.beforeStop()
  }

  async dial (ma: Multiaddr, options: TCPDialOptions): Promise<Connection> {
    if (!this.acceptingDials) {
      throw new NotStartedError('TCP transport is not started')
    }

    // Defer execution until after the completion promise is registered. This
    // prevents progress callbacks and synchronous DNS lookups from starting
    // shutdown before this dial is represented in pendingDialOperations.
    const operation = Promise.resolve().then(async () => {
      const signal = anySignal([
        options.signal,
        this.shutdownController.signal
      ])

      try {
        return await this.performDial(ma, {
          ...options,
          signal
        })
      } finally {
        signal.clear()
      }
    })
    const completion = operation.then(() => undefined, () => undefined)

    this.pendingDialOperations.add(completion)
    void completion.finally(() => {
      this.pendingDialOperations.delete(completion)
    })

    return operation
  }

  private async performDial (ma: Multiaddr, options: TCPDialOptions): Promise<Connection> {
    options.keepAlive = options.keepAlive ?? true
    options.noDelay = options.noDelay ?? true
    options.allowHalfOpen = options.allowHalfOpen ?? false

    // options.signal destroys the socket before 'connect' event
    const socket = await this._connect(ma, options)

    let maConn: MultiaddrConnection

    try {
      maConn = toMultiaddrConnection({
        socket,
        inactivityTimeout: this.opts.outboundSocketInactivityTimeout,
        metrics: this.metrics?.events,
        direction: 'outbound',
        remoteAddr: ma,
        log: this.log.newScope('connection')
      })
    } catch (err: any) {
      this.metrics?.errors.increment({ outbound_to_connection: true })
      await closeSocket(socket)
      throw err
    }

    try {
      this.log('new outbound connection %s', maConn.remoteAddr)
      const connection = await options.upgrader.upgradeOutbound(maConn, options)
      options.signal.throwIfAborted()

      return connection
    } catch (err: any) {
      this.metrics?.errors.increment({ outbound_upgrade: true })
      this.log.error('error upgrading outbound connection - %e', err)
      const closed = socket.closed
        ? Promise.resolve()
        : new Promise<void>((resolve) => {
          socket.once('close', () => {
            resolve()
          })
        })
      maConn.abort(err)
      await closed
      throw err
    }
  }

  async _connect (ma: Multiaddr, options: TCPDialOptions): Promise<Socket> {
    options.signal.throwIfAborted()
    options.onProgress?.(new CustomProgressEvent('tcp:open-connection'))
    options.signal.throwIfAborted()

    let rawSocket: Socket

    return new Promise<Socket>((resolve, reject) => {
      let settled = false
      const start = Date.now()
      const cOpts = multiaddrToNetConfig(ma, {
        ...(this.opts.dialOpts ?? {}),
        ...options
      }) as (IpcSocketConnectOpts & TcpSocketConnectOpts)

      this.log('dialing %a with opts %o', ma, cOpts)
      rawSocket = net.connect(cOpts)

      const onError = (err: Error): void => {
        this.log.error('dial to %a errored - %e', ma, err)
        const cOptsStr = cOpts.path ?? `${cOpts.host ?? ''}:${cOpts.port}`
        err.message = `connection error ${cOptsStr}: ${err.message}`
        this.metrics?.events.increment({ error: true })
        done(err)
      }

      const onTimeout = (): void => {
        this.log('connection timeout %a', ma)
        this.metrics?.events.increment({ timeout: true })

        const err = new TimeoutError(`Connection timeout after ${Date.now() - start}ms`)
        // Note: this will result in onError() being called
        rawSocket.emit('error', err)
      }

      const onConnect = (): void => {
        this.log('connection opened %a', ma)
        this.metrics?.events.increment({ connect: true })
        done()
      }

      const onAbort = (): void => {
        this.log('connection aborted %a', ma)
        this.metrics?.events.increment({ abort: true })
        done(options.signal.reason instanceof Error ? options.signal.reason : new AbortError())
      }

      const done = (err?: Error): void => {
        if (settled) {
          return
        }

        settled = true
        rawSocket.removeListener('error', onError)
        rawSocket.removeListener('timeout', onTimeout)
        rawSocket.removeListener('connect', onConnect)

        if (options.signal != null) {
          options.signal.removeEventListener('abort', onAbort)
        }

        if (err != null) {
          reject(err); return
        }

        resolve(rawSocket)
      }

      rawSocket.on('error', onError)
      rawSocket.on('timeout', onTimeout)
      rawSocket.on('connect', onConnect)

      options.signal.addEventListener('abort', onAbort, { once: true })

      // net.connect() can invoke a user-supplied lookup function before it
      // returns. If that function started shutdown, the abort event occurred
      // before the listener above was installed.
      if (options.signal.aborted) {
        onAbort()
      }
    })
      .catch(async err => {
        if (rawSocket != null) {
          await closeSocket(rawSocket)
        }

        throw err
      })
  }

  /**
   * Creates a TCP listener. The provided `handler` function will be called
   * anytime a new incoming Connection has been successfully upgraded via
   * `upgrader.upgradeInbound`.
   */
  createListener (options: TCPCreateListenerOptions): Listener {
    return new TCPListener({
      ...(this.opts.listenOpts ?? {}),
      ...options,
      maxConnections: this.opts.maxConnections,
      backlog: this.opts.backlog,
      closeServerOnMaxConnections: this.opts.closeServerOnMaxConnections,
      inactivityTimeout: this.opts.inboundSocketInactivityTimeout,
      metrics: this.components.metrics,
      logger: this.components.logger
    })
  }

  /**
   * Takes a list of `Multiaddr`s and returns only valid TCP addresses
   */
  listenFilter (multiaddrs: Multiaddr[]): Multiaddr[] {
    return multiaddrs.filter(ma => TCPMatcher.exactMatch(ma) || ma.toString().startsWith('/unix/'))
  }

  /**
   * Filter check for all Multiaddrs that this transport can dial
   */
  dialFilter (multiaddrs: Multiaddr[]): Multiaddr[] {
    return this.listenFilter(multiaddrs)
  }
}

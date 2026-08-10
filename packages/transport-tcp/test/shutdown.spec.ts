import net from 'node:net'
import { stop } from '@libp2p/interface'
import { defaultLogger } from '@libp2p/logger'
import { multiaddr } from '@multiformats/multiaddr'
import { expect } from 'aegir/chai'
import pDefer from 'p-defer'
import pWaitFor from 'p-wait-for'
import Sinon from 'sinon'
import { stubInterface } from 'sinon-ts'
import { tcp } from '../src/index.ts'
import type { Connection, Transport, Upgrader } from '@libp2p/interface'

class TestSocket extends net.Socket {
  destroyCalls = 0

  override destroy (): this {
    this.destroyCalls++
    return this
  }

  override resetAndDestroy (): this {
    this.destroyCalls++
    return this
  }

  close (): void {
    this.emit('close', false)
  }
}

describe('shutdown', () => {
  afterEach(() => {
    Sinon.restore()
  })

  it('should destroy a pending outbound socket and await close', async () => {
    const socket = new TestSocket()
    const connect = Sinon.stub(net, 'connect').returns(socket)
    const transport = tcp()({
      logger: defaultLogger()
    })
    const upgrader = stubInterface<Upgrader>()
    const addr = multiaddr('/ip4/127.0.0.1/tcp/9000')
    const dialPromise = transport.dial(addr, {
      upgrader,
      signal: AbortSignal.timeout(5_000)
    })

    await Promise.resolve()

    let stopped = false
    const stopPromise = stop(transport).then(() => {
      stopped = true
    })

    await Promise.resolve()

    expect(connect.calledOnce).to.be.true()
    expect(socket.destroyCalls).to.equal(1)
    expect(stopped).to.be.false()

    socket.close()
    await stopPromise

    expect(stopped).to.be.true()
    await expect(dialPromise)
      .to.eventually.be.rejected()
      .and.to.have.property('name', 'AbortError')
  })

  it('should register a dial before invoking progress callbacks', async () => {
    const connect = Sinon.stub(net, 'connect')
    const transport = tcp()({
      logger: defaultLogger()
    })
    const upgrader = stubInterface<Upgrader>()
    let stopPromise: Promise<void> | undefined
    const dialPromise = transport.dial(multiaddr('/ip4/127.0.0.1/tcp/9000'), {
      upgrader,
      signal: AbortSignal.timeout(5_000),
      onProgress: () => {
        stopPromise = stop(transport)
      }
    })

    await expect(dialPromise)
      .to.eventually.be.rejected()
      .and.to.have.property('name', 'AbortError')
    await stopPromise

    expect(connect.called).to.be.false()
  })

  it('should close a socket returned while shutdown starts inside net.connect', async () => {
    const socket = new TestSocket()
    const transport = tcp()({
      logger: defaultLogger()
    })
    const upgrader = stubInterface<Upgrader>()
    let stopPromise: Promise<void> | undefined
    const connect = Sinon.stub(net, 'connect').callsFake(() => {
      stopPromise = stop(transport)
      return socket
    })
    const dialPromise = transport.dial(multiaddr('/ip4/127.0.0.1/tcp/9000'), {
      upgrader,
      signal: AbortSignal.timeout(5_000)
    })

    await Promise.resolve()
    await Promise.resolve()

    expect(connect.calledOnce).to.be.true()
    expect(socket.destroyCalls).to.equal(1)

    socket.close()
    await stopPromise

    await expect(dialPromise)
      .to.eventually.be.rejected()
      .and.to.have.property('name', 'AbortError')
  })

  it('should abort an outbound upgrade and await socket close', async () => {
    const socket = new TestSocket()
    const upgradeStarted = pDefer<void>()
    Sinon.stub(net, 'connect').returns(socket)
    const transport = tcp()({
      logger: defaultLogger()
    })
    const upgrader = stubInterface<Upgrader>({
      upgradeOutbound: async (_maConn, options) => {
        upgradeStarted.resolve()

        await new Promise<void>((resolve, reject) => {
          options?.signal?.addEventListener('abort', () => {
            reject(options.signal?.reason)
          }, { once: true })
        })

        return stubInterface<Connection>()
      }
    })
    const dialPromise = transport.dial(multiaddr('/ip4/127.0.0.1/tcp/9000'), {
      upgrader,
      signal: AbortSignal.timeout(5_000)
    })

    await Promise.resolve()
    socket.emit('connect')
    await upgradeStarted.promise

    let stopped = false
    const stopPromise = stop(transport).then(() => {
      stopped = true
    })

    await pWaitFor(() => socket.destroyCalls === 1)

    expect(socket.destroyCalls).to.equal(1)
    expect(stopped).to.be.false()

    socket.close()
    await stopPromise

    expect(stopped).to.be.true()
    await expect(dialPromise).to.eventually.be.rejected()
  })

  it('should not close successfully upgraded sockets', async () => {
    const socket = new TestSocket()
    Sinon.stub(net, 'connect').returns(socket)
    const transport = tcp()({
      logger: defaultLogger()
    })
    const connection = stubInterface<Connection>()
    const upgrader = stubInterface<Upgrader>({
      upgradeOutbound: Sinon.stub().resolves(connection)
    })
    const dialPromise = transport.dial(multiaddr('/ip4/127.0.0.1/tcp/9000'), {
      upgrader,
      signal: AbortSignal.timeout(5_000)
    })

    await Promise.resolve()
    socket.emit('connect')

    await expect(dialPromise).to.eventually.equal(connection)
    await stop(transport)

    expect(socket.destroyCalls).to.equal(0)
  })

  it('should reject dials after shutdown', async () => {
    const transport: Transport = tcp()({
      logger: defaultLogger()
    })

    await stop(transport)

    await expect(transport.dial(multiaddr('/ip4/127.0.0.1/tcp/9000'), {
      upgrader: stubInterface<Upgrader>(),
      signal: AbortSignal.timeout(5_000)
    }))
      .to.eventually.be.rejected()
      .and.to.have.property('name', 'NotStartedError')
  })
})

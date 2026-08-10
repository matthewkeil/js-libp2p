import { expect } from 'aegir/chai'
import pDefer from 'p-defer'
import Sinon from 'sinon'
import { createLibp2p } from '../../src/index.ts'
import type { Startable } from '@libp2p/interface'

describe('shutdown', () => {
  it('should reject negative post-drain timeouts', async () => {
    await expect(createLibp2p({
      transportManager: {
        shutdownPostDrainTimeout: -1
      }
    }))
      .to.eventually.be.rejected()
      .and.to.have.property('name', 'InvalidParametersError')
  })

  it('should wait after all components drain and before teardown', async () => {
    const calls: string[] = []
    const drained = pDefer<void>()
    const node = await createLibp2p<{ lifecycle: Startable }>({
      connectionMonitor: {
        enabled: false
      },
      transportManager: {
        shutdownPostDrainTimeout: 50
      },
      services: {
        lifecycle: () => ({
          start: () => {},
          beforeStop: async () => {
            calls.push('beforeStop')
            await drained.promise
            calls.push('drained')
          },
          stop: () => {
            calls.push('stop')
          }
        })
      }
    })
    const clock = Sinon.useFakeTimers()

    try {
      const stopPromise = node.stop()

      await clock.tickAsync(100)
      expect(calls).to.deep.equal(['beforeStop'])

      drained.resolve()
      await clock.tickAsync(0)
      expect(calls).to.deep.equal(['beforeStop', 'drained'])

      await clock.tickAsync(49)
      expect(calls).to.deep.equal(['beforeStop', 'drained'])

      await clock.tickAsync(1)
      await stopPromise
      expect(calls).to.deep.equal(['beforeStop', 'drained', 'stop'])
    } finally {
      clock.restore()
    }
  })
})

import { describe, expect, it } from 'vitest'
import * as charts from './index'

describe('charts index', () => {
  it('exports every chart primitive', () => {
    expect(Object.keys(charts).sort()).toEqual([
      'ArcGauge',
      'Bullet',
      'RunBars',
      'RunDots',
      'SegmentRing',
      'Sparkline',
      'StackedArea',
    ])
  })
})

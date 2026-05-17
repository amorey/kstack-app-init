import { describe, expect, it } from 'vitest';

import { errorMessage, toError } from './error-bus';

describe('toError', () => {
  it('passes an Error through unchanged (preserves the stack)', () => {
    const e = new Error('boom');
    expect(toError(e)).toBe(e);
  });

  it('preserves Error subclasses', () => {
    const e = new TypeError('nope');
    expect(toError(e)).toBe(e);
  });

  it('wraps a non-Error value via String()', () => {
    const e = toError('plain string');
    expect(e).toBeInstanceOf(Error);
    expect(e.message).toBe('plain string');
  });

  it('wraps null/undefined without throwing', () => {
    expect(toError(undefined).message).toBe('undefined');
    expect(toError(null).message).toBe('null');
  });
});

describe('errorMessage', () => {
  it("returns an Error's message (not its 'Error: ' toString)", () => {
    expect(errorMessage(new Error('the message'))).toBe('the message');
  });

  it('stringifies a non-Error value', () => {
    expect(errorMessage('raw')).toBe('raw');
    expect(errorMessage(42)).toBe('42');
  });
});

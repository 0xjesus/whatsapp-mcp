"""Detect invalid source checkpoints without reading message bodies or mutating state."""


def recovery_plan(state, source):
    """Call after install_capture, before resuming ingestion, including while running.

    On reset_required, atomically clear this source's derived messages/checkpoints
    and register source_identity before starting its backfill. A committed CDC
    batch must store its final event_token as change_token with change_seq.
    This check assumes the capture queue retains the checkpoint event.
    """
    identity = source.identity()
    previous_identity = state.get('source_identity')
    seq = int(state.get('change_seq') or 0)
    reason = None
    if not previous_identity:
        reason = 'source_identity_missing'
    elif previous_identity != identity:
        reason = 'source_identity_changed'
    elif seq > 0:
        token = source.checkpoint_token(seq)
        if token is None:
            reason = 'checkpoint_missing'
        elif not state.get('change_token'):
            reason = 'checkpoint_token_missing'
        elif token != state['change_token']:
            reason = 'checkpoint_reused'
    return dict(reset_required=reason is not None, reason=reason, source_identity=identity)

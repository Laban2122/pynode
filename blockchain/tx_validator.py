from io import BytesIO
from core.helpers import parse_tx_full, get_tx_output

TAG_VALID   = 'valid'    # all inputs resolved, outputs <= inputs
TAG_PARTIAL = 'partial'  # one or more inputs unresolvable (pre-range or mempool)
TAG_INVALID = 'invalid'  # all inputs resolved, outputs > inputs


def validate_txs(raw_txs: list, tx_index, block_chain) -> list[dict]:
    """
    Validate a list of raw transactions (bytes or hex strings).

    Each result dict:
        txid            — transaction id (display format)
        tag             — TAG_VALID / TAG_PARTIAL / TAG_INVALID
        inputs_total    — number of inputs (0 for coinbase)
        inputs_resolved — inputs whose UTXO was found
        inputs_sum_sat  — total input satoshis (None when unresolved inputs exist)
        outputs_sum_sat — total output satoshis
        fee_sat         — fee in satoshis (None when inputs_sum is unknown)
        detail          — human-readable explanation for partial/invalid
    """
    return [_validate_one(raw, tx_index, block_chain) for raw in raw_txs]


def _validate_one(raw, tx_index, block_chain) -> dict:
    if isinstance(raw, str):
        raw = bytes.fromhex(raw)

    tx = parse_tx_full(BytesIO(raw))

    if tx['is_coinbase']:
        return {
            'txid': tx['txid'],
            'tag': TAG_VALID,
            'inputs_total': 0,
            'inputs_resolved': 0,
            'inputs_sum_sat': None,
            'outputs_sum_sat': sum(o['value'] for o in tx['outputs']),
            'fee_sat': None,
            'detail': 'coinbase',
        }

    outputs_sum = sum(o['value'] for o in tx['outputs'])
    inputs_sum  = 0
    unresolved  = []

    for inp in tx['inputs']:
        height = tx_index.get_block_height_from_hex(inp['prev_txid'])
        if height is None:
            unresolved.append(f"{inp['prev_txid'][:16]}…:{inp['prev_vout']}")
            continue

        block_data = block_chain.get_block_by_height(height)
        if not block_data:
            unresolved.append(f"{inp['prev_txid'][:16]}…:{inp['prev_vout']}")
            continue

        amount = get_tx_output(block_data, inp['prev_txid'], inp['prev_vout'])
        if amount is None:
            unresolved.append(f"{inp['prev_txid'][:16]}…:{inp['prev_vout']}")
            continue

        inputs_sum += amount

    inputs_resolved = len(tx['inputs']) - len(unresolved)

    if unresolved:
        tag    = TAG_PARTIAL
        fee    = None
        detail = 'unresolved inputs (pre-range or mempool): ' + ', '.join(unresolved)
    elif outputs_sum > inputs_sum:
        tag    = TAG_INVALID
        fee    = inputs_sum - outputs_sum  # negative
        detail = f'outputs ({outputs_sum} sat) exceed inputs ({inputs_sum} sat)'
    else:
        tag    = TAG_VALID
        fee    = inputs_sum - outputs_sum
        detail = ''

    return {
        'txid': tx['txid'],
        'tag': tag,
        'inputs_total': len(tx['inputs']),
        'inputs_resolved': inputs_resolved,
        'inputs_sum_sat': inputs_sum if not unresolved else None,
        'outputs_sum_sat': outputs_sum,
        'fee_sat': fee,
        'detail': detail,
    }

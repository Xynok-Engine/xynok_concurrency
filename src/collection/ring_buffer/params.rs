use crate::sync::Ordering;
use crate::utils::cursors::CursorData;
pub struct ParamsCasTailForPushBatch<'a>
{
    pub cursor_data:      &'a mut CursorData,
    pub src_amount:       usize,
    pub push_amount:      usize,
    pub success_order:    Ordering,
    pub fail_order:       Ordering,
    pub fetch_head_order: Ordering,
}

pub struct ParamsCasForPopBatch<'a>
{
    pub cursor_data:           &'a mut CursorData,
    pub pop_amount:            usize,
    pub success_order:         Ordering,
    pub fail_order:            Ordering,
    pub fetch_after_cas_order: Ordering,
}

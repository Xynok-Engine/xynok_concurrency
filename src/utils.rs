#[inline]
pub(crate) fn ignore_poison<G>(result: std::sync::LockResult<G>) -> G
{
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

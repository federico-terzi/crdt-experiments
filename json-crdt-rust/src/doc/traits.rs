use crate::{transaction::Transaction, DataMap, Doc, ObjRef, Selector, Value};

use super::doc::DocError;

pub trait ReadableDoc {
    fn get<TRef: Into<ObjRef>, TSelector: Into<Selector>>(
        &self,
        object: TRef,
        selector: TSelector,
    ) -> Result<Option<&Value>, DocError>;
    fn get_text<TRef: Into<ObjRef>>(&self, object: TRef) -> Result<Option<String>, DocError>;
    fn as_map<'a>(&'a self) -> Result<DataMap<'a>, DocError>;
}

pub trait WritableDoc {
    fn merge(&mut self, other: &Doc) -> Result<(), DocError>;
    fn transaction(&mut self) -> Transaction;
}

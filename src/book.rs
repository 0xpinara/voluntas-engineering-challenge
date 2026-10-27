//! A price-level book stored as two ascending vectors. Prediction market books are shallow (at
//! most a few hundred ticks), so sorted vectors beat trees on memory and cache locality, which
//! matters with ~150k books times one copy per socket.

use anyhow::ensure;

use crate::venue::{Level, Side};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

impl Book {
    pub fn new(mut bids: Vec<(i64, i64)>, mut asks: Vec<(i64, i64)>) -> Self {
        for side in [&mut bids, &mut asks] {
            side.retain(|l| l.1 > 0);
            side.sort_unstable();
            side.dedup_by_key(|l| l.0);
        }
        Self { bids, asks }
    }

    pub fn side(&self, side: Side) -> &[(i64, i64)] {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    pub fn qty(&self, side: Side, px: i64) -> i64 {
        let v = self.side(side);
        v.binary_search_by_key(&px, |l| l.0).map_or(0, |i| v[i].1)
    }

    /// Applies a change and returns the level's new absolute size.
    pub fn apply(&mut self, l: &Level, rel: bool) -> anyhow::Result<i64> {
        let v = match l.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let at = v.binary_search_by_key(&l.px, |e| e.0);
        let q = match at {
            Ok(i) if rel => v[i].1 + l.qty,
            _ => l.qty,
        };
        ensure!(
            q >= 0,
            "{:?} level {} would go negative ({q})",
            l.side,
            l.px
        );
        match at {
            Ok(i) if q == 0 => {
                v.remove(i);
            }
            Ok(i) => v[i].1 = q,
            Err(i) if q > 0 => v.insert(i, (l.px, q)),
            Err(_) => {}
        }
        Ok(q)
    }

    /// The absolute level changes that turn `self` into `to`.
    pub fn diff(&self, to: &Book, out: &mut Vec<Level>) {
        for side in [Side::Buy, Side::Sell] {
            let (a, b) = (self.side(side), to.side(side));
            let (mut i, mut j) = (0, 0);
            loop {
                match (a.get(i), b.get(j)) {
                    (Some(x), Some(y)) if x.0 == y.0 => {
                        if x.1 != y.1 {
                            out.push(Level {
                                side,
                                px: y.0,
                                qty: y.1,
                            });
                        }
                        i += 1;
                        j += 1;
                    }
                    (Some(x), Some(y)) if x.0 < y.0 => {
                        out.push(Level {
                            side,
                            px: x.0,
                            qty: 0,
                        });
                        i += 1;
                    }
                    (Some(x), None) => {
                        out.push(Level {
                            side,
                            px: x.0,
                            qty: 0,
                        });
                        i += 1;
                    }
                    (_, Some(y)) => {
                        out.push(Level {
                            side,
                            px: y.0,
                            qty: y.1,
                        });
                        j += 1;
                    }
                    (None, None) => break,
                }
            }
        }
    }

    pub fn crossed(&self) -> bool {
        matches!((self.bids.last(), self.asks.first()), (Some(b), Some(a)) if b.0 >= a.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(side: Side, px: i64, qty: i64) -> Level {
        Level { side, px, qty }
    }

    #[test]
    fn relative_and_absolute_updates() {
        let mut b = Book::new(vec![(40, 5), (30, 1)], vec![(60, 2)]);
        assert_eq!(b.apply(&lv(Side::Buy, 40, -2), true).unwrap(), 3);
        assert_eq!(b.apply(&lv(Side::Buy, 30, -1), true).unwrap(), 0);
        assert_eq!(b.apply(&lv(Side::Sell, 55, 7), false).unwrap(), 7);
        assert_eq!(b, Book::new(vec![(40, 3)], vec![(55, 7), (60, 2)]));
        assert!(b.apply(&lv(Side::Sell, 60, -3), true).is_err());
    }

    #[test]
    fn diff_round_trips() {
        let a = Book::new(vec![(10, 1), (20, 2), (30, 3)], vec![(50, 1)]);
        let b = Book::new(vec![(20, 9), (25, 1)], vec![(50, 1), (70, 4)]);
        let mut d = Vec::new();
        a.diff(&b, &mut d);
        let mut c = a.clone();
        for l in &d {
            c.apply(l, false).unwrap();
        }
        assert_eq!(c, b);
        assert_eq!(d.len(), 5);
    }
}

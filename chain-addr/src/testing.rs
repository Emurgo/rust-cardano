use crate::{Address, AddressReadable, Discrimination, Kind, KindType};
use quickcheck::{Arbitrary, Gen};

impl Arbitrary for Discrimination {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        match u8::arbitrary(g) % 2 {
            0 => Discrimination::Production,
            1 => Discrimination::Test,
            _ => unreachable!(),
        }
    }
}

impl Arbitrary for KindType {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => KindType::Single,
            1 => KindType::Group,
            2 => KindType::Account,
            3 => KindType::Multisig,
            _ => unreachable!(),
        }
    }
}

impl Arbitrary for AddressReadable {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        AddressReadable::from_address(&Arbitrary::arbitrary(g))
    }
}
impl Arbitrary for Address {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let discrimination = Arbitrary::arbitrary(g);
        let kind = match KindType::arbitrary(g) {
            KindType::Single => Kind::Single(Arbitrary::arbitrary(g)),
            KindType::Group => Kind::Group(Arbitrary::arbitrary(g), Arbitrary::arbitrary(g)),
            KindType::Account => Kind::Account(Arbitrary::arbitrary(g)),
            KindType::Multisig => {
                let mut h = [0u8; 32];
                for i in h.iter_mut() {
                    *i = Arbitrary::arbitrary(g)
                }
                Kind::Multisig(h)
            }
        };
        Address(discrimination, kind)
    }
}

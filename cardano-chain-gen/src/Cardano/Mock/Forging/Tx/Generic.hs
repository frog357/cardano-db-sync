
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Cardano.Mock.Forging.Tx.Generic
  ( allPoolStakeCert
  , resolveUTxOIndex
  , resolveStakeCreds
  ) where

import           Cardano.Prelude hiding ((.), length)

import           Data.List (nub)
import qualified Data.Set as Set
import qualified Data.Map.Strict as Map

import           Cardano.Ledger.Address
import qualified Cardano.Ledger.Core as Core
import           Cardano.Ledger.Credential
import           Cardano.Ledger.Era (Crypto)
import           Cardano.Ledger.Shelley.LedgerState hiding (LedgerState)
import           Cardano.Ledger.Shelley.TxBody hiding (TxIn)
import           Cardano.Ledger.Shelley.UTxO
import           Cardano.Ledger.TxIn (TxIn (..))

import           Ouroboros.Consensus.Cardano.Block (LedgerState)
import           Ouroboros.Consensus.Shelley.Eras (StandardCrypto)
import           Ouroboros.Consensus.Shelley.Ledger (ShelleyBlock)
import qualified Ouroboros.Consensus.Shelley.Ledger.Ledger as Consensus

import           Cardano.Mock.Forging.Types

resolveUTxOIndex :: forall era. (Crypto era ~ StandardCrypto, HasField "address" (Core.TxOut era) (Addr (Crypto era)))
                 => UTxOIndex -> LedgerState (ShelleyBlock era)
                 -> Either ForgingError ((TxIn (Crypto era), Core.TxOut era), UTxOIndex)
resolveUTxOIndex index st = toLeft $ case index of
    UTxOIndex n -> safeIndex n utxoPairs
    UTxOAddress addr -> find (eq addr) utxoPairs
  where
    utxoPairs :: [(TxIn (Crypto era), Core.TxOut era)]
    utxoPairs = Map.toList $ unUTxO $ _utxo $ _utxoState $ esLState $
        nesEs $ Consensus.shelleyLedgerState st

    safeIndex :: Int -> [a] -> Maybe a
    safeIndex n ls
      | n < length ls = Just $ ls !! n
      | True = Nothing

    eq addr (_, txOut) = addr == getField @"address" txOut

    toLeft :: Maybe (TxIn (Crypto era), Core.TxOut era) -> Either ForgingError ((TxIn (Crypto era), Core.TxOut era), UTxOIndex)
    toLeft Nothing = Left $ CantFindUTxO index
    toLeft (Just (txIn, txOut)) = Right ((txIn, txOut), UTxOAddress (getField @"address" txOut))

resolveStakeCreds :: StakeIndex -> LedgerState (ShelleyBlock era) -> Either ForgingError (StakeCredential (Crypto era))
resolveStakeCreds (StakeIndex n) st = Right $ rewardAccs !! n
  where
    rewardAccs = Map.keys $ _rewards $ _dstate $ _delegationState $ esLState $
        nesEs $ Consensus.shelleyLedgerState st

allPoolStakeCert :: LedgerState (ShelleyBlock era) -> [DCert (Crypto era)]
allPoolStakeCert st =
    DCertDeleg . RegKey <$> nub creds
  where
    poolParms = Map.elems $ _pParams $ _pstate $ _delegationState $
      esLState $ nesEs $ Consensus.shelleyLedgerState st
    creds = concatMap getPoolStakeCreds poolParms

getPoolStakeCreds :: PoolParams c -> [StakeCredential c]
getPoolStakeCreds pparams =
     (getRwdCred (_poolRAcnt pparams)) :
      (KeyHashObj <$> Set.toList (_poolOwners pparams))

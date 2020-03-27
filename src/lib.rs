// Copyright (c) 2019-2020 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Create a [`Sink`] implementation from an initial value and a closure
//! returning a [`Future`].
//!
//! This is very similar to how `futures::stream::unfold` creates a `Stream`
//! implementation from a seed value and a future-returning closure.
//!
//! # Examples
//!
//! ```no_run
//! use async_std::io;
//! use futures::prelude::*;
//! use quicksink::Action;
//!
//! quicksink::make_sink(io::stdout(), |mut stdout, action| async move {
//!     match action {
//!         Action::Send(x) => stdout.write_all(x).await?,
//!         Action::Flush => stdout.flush().await?,
//!         Action::Close => stdout.close().await?
//!     }
//!     Ok::<_, io::Error>(stdout)
//! });
//! ```
//!
//! # Error behaviour
//!
//! If any of the [`Sink`] methods produce an error, the sink transitions to
//! a failure state. Subsequent `poll_ready`, `poll_flush` or `poll_close`
//! calls return [`Error::Closed`]. Invoking `start_send` at this point
//! violates the API contract of the [`Sink`] trait because the preceding
//! call to `poll_ready` was not successful and causes an assertion failure.
//!
//! # Closing behaviour
//!
//! If the sink is closed regularly, subsequent calls to `poll_ready` will
//! return [`Error::Closed`] and `start_send` will produce an assertion
//! failure because the preceding call to `poll_ready` was not successful.
//! Further `poll_flush` and `poll_close` calls will have no effect.
//!

use futures_core::ready;
use futures_sink::Sink;
use pin_project_lite::pin_project;
use std::{
    fmt::{self, Display, Formatter},
    error,
    future::Future,
    pin::Pin,
    task::{Context, Poll}
};

/// Returns a `Sink` impl based on the initial value and the given closure.
///
/// The closure will be applied to the initial value and an [`Action`] that
/// informs it about the action it should perform. The returned [`Future`]
/// will resolve to another value and the process starts over using this
/// output.
pub fn make_sink<S, F, T, A, E>(init: S, f: F) -> SinkImpl<S, F, T, A, E>
where
    F: FnMut(S, Action<A>) -> T,
    T: Future<Output = Result<S, E>>,
{
    SinkImpl {
        lambda: f,
        future: None,
        param: Some(init),
        state: State::Empty,
        _mark: std::marker::PhantomData
    }
}

/// The command given to the closure so that it can perform appropriate action.
///
/// Presumably the closure encapsulates a resource to perform I/O. The commands
/// correspond to methods of the [`Sink`] trait and provide the closure with
/// sufficient information to know what kind of action to perform with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<A> {
    /// Send the given value.
    /// Corresponds to [`Sink::start_send`].
    Send(A),
    /// Flush the resource.
    /// Corresponds to [`Sink::poll_flush`].
    Flush,
    /// Close the resource.
    /// Corresponds to [`Sink::poll_close`].
    Close
}

/// The various states the `Sink` may be in.
#[derive(Debug, PartialEq, Eq)]
enum State {
    /// The `Sink` is idle.
    Empty,
    /// The `Sink` is sending a value.
    Sending,
    /// The `Sink` is flushing its resource.
    Flushing,
    /// The `Sink` is closing its resource.
    Closing,
    /// The `Sink` is closed (terminal state).
    Closed,
    /// The `Sink` experienced an error (terminal state).
    Failed
}

pin_project!
{
    /// `SinkImpl` implements the `Sink` trait.
    #[derive(Debug)]
    pub struct SinkImpl<S, F, T, A, E> {
        lambda: F,
        #[pin] future: Option<T>,
        param: Option<S>,
        state: State,
        _mark: std::marker::PhantomData<(A, E)>
    }
}

impl<S, F, T, A, E> Sink<A> for SinkImpl<S, F, T, A, E>
where
    F: FnMut(S, Action<A>) -> T,
    T: Future<Output = Result<S, E>>
{
    type Error = Error<E>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        match this.state {
            State::Sending | State::Flushing => {
                match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                    Ok(p) => {
                        this.future.set(None);
                        *this.param = Some(p);
                        *this.state = State::Empty;
                        Poll::Ready(Ok(()))
                    }
                    Err(e) => {
                        this.future.set(None);
                        *this.state = State::Failed;
                        Poll::Ready(Err(Error::Inner(e)))
                    }
                }
            }
            State::Closing => {
                match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                    Ok(_) => {
                        this.future.set(None);
                        *this.state = State::Closed;
                        Poll::Ready(Err(Error::Closed))
                    }
                    Err(e) => {
                        this.future.set(None);
                        *this.state = State::Failed;
                        Poll::Ready(Err(Error::Inner(e)))
                    }
                }
            }
            State::Empty => {
                assert!(this.param.is_some());
                Poll::Ready(Ok(()))
            }
            State::Closed | State::Failed => Poll::Ready(Err(Error::Closed))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: A) -> Result<(), Self::Error> {
        assert_eq!(State::Empty, self.state);
        let mut this = self.project();
        let param = this.param.take().unwrap();
        let future = (this.lambda)(param, Action::Send(item));
        this.future.set(Some(future));
        *this.state = State::Sending;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        loop {
            let mut this = self.as_mut().project();
            match this.state {
                State::Empty =>
                    if let Some(p) = this.param.take() {
                        let future = (this.lambda)(p, Action::Flush);
                        this.future.set(Some(future));
                        *this.state = State::Flushing
                    } else {
                        return Poll::Ready(Ok(()))
                    }
                State::Sending =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            this.future.set(None);
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Flushing =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            this.future.set(None);
                            *this.param = Some(p);
                            *this.state = State::Empty;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Closing =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(_) => {
                            this.future.set(None);
                            *this.state = State::Closed;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Closed => return Poll::Ready(Ok(())),
                State::Failed => return Poll::Ready(Err(Error::Closed))
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        loop {
            let mut this = self.as_mut().project();
            match this.state {
                State::Empty =>
                    if let Some(p) = this.param.take() {
                        let future = (this.lambda)(p, Action::Close);
                        this.future.set(Some(future));
                        *this.state = State::Closing;
                    } else {
                        return Poll::Ready(Ok(()))
                    }
                State::Sending =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            this.future.set(None);
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Flushing =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(p) => {
                            this.future.set(None);
                            *this.param = Some(p);
                            *this.state = State::Empty
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Closing =>
                    match ready!(this.future.as_mut().as_pin_mut().unwrap().poll(cx)) {
                        Ok(_) => {
                            this.future.set(None);
                            *this.state = State::Closed;
                            return Poll::Ready(Ok(()))
                        }
                        Err(e) => {
                            this.future.set(None);
                            *this.state = State::Failed;
                            return Poll::Ready(Err(Error::Inner(e)))
                        }
                    }
                State::Closed => return Poll::Ready(Ok(())),
                State::Failed => return Poll::Ready(Err(Error::Closed))
            }
        }
    }
}

/// Possible errors of [`SinkImpl`].
#[derive(Debug, Clone)]
pub enum Error<E> {
    /// Error caused by the underlying future.
    Inner(E),
    /// An operation on a closed sink has been attempted.
    Closed
}

impl<E> Error<E> {
    /// Predicate to check for the `Closed` error case.
    pub fn is_closed(&self) -> bool {
        if let Error::Closed = self {
            true
        } else {
            false
        }
    }

    /// Predicate to check for the `Inner` error case.
    pub fn is_inner(&self) -> bool {
        if let Error::Inner(_) = self {
            true
        } else {
            false
        }
    }

    /// Return the inner error if any.
    pub fn into_inner(self) -> Option<E> {
        if let Error::Inner(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl<E: Display> Display for Error<E> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Inner(e) => e.fmt(f),
            Error::Closed => f.write_str("sink is closed")
        }
    }
}

impl<E: error::Error + 'static> error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Inner(e) => Some(e),
            Error::Closed => None
        }
    }
}

#[cfg(test)]
mod tests {
    use async_std::{io, task};
    use futures::{channel::mpsc, prelude::*, stream};
    use crate::{Action, make_sink};

    #[test]
    fn smoke_test() {
        task::block_on(async {
            let sink = make_sink(io::stdout(), |mut stdout, action| async move {
                match action {
                    Action::Send(x) => stdout.write_all(x).await?,
                    Action::Flush => stdout.flush().await?,
                    Action::Close => stdout.close().await?
                }
                Ok::<_, io::Error>(stdout)
            });

            let values = vec![Ok(&b"hello\n"[..]), Ok(&b"world\n"[..])];
            assert!(stream::iter(values).forward(sink).await.is_ok())
        })
    }

    #[test]
    fn replay() {
        task::block_on(async {
            let (tx, rx) = mpsc::unbounded();

            let sink = make_sink(tx, |mut tx, action| async move {
                tx.send(action.clone()).await?;
                if action == Action::Close {
                    tx.close().await?
                }
                Ok::<_, mpsc::SendError>(tx)
            });

            futures::pin_mut!(sink);

            let expected = [
                Action::Send("hello\n"),
                Action::Flush,
                Action::Send("world\n"),
                Action::Flush,
                Action::Close
            ];

            for &item in &["hello\n", "world\n"] {
                sink.send(item).await.unwrap()
            }

            sink.close().await.unwrap();

            let actual = rx.collect::<Vec<_>>().await;

            assert_eq!(&expected[..], &actual[..])
        });
    }
}
